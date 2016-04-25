package nbd

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"github.com/mattn/go-isatty"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	BLOCKBITS = 9
	BLOCKSIZE = (1 << BLOCKBITS)
)

type IntegrityTest struct {
	wg       sync.WaitGroup
	rxwg     sync.WaitGroup
	commands *bufio.Reader
	ni       *NbdInstance

	inflight       map[uint64]Inflight
	inflightBlk    map[uint64]bool
	inflightCond   *sync.Cond
	lastHandle     map[uint64]uint64
	disconnectSent bool
	inflightMutex  sync.Mutex // protects the above block

	quit         chan struct{}
	tickerQuit   chan struct{}
	send         chan nbdRequest
	errs         chan error
	aborted      bool
	abortedMutex sync.Mutex
	statRead     uint64
	statSent     uint64
	statRecv     uint64
	statBytes    uint64
	statBlocked  uint64
	statStart    time.Time
}

type Inflight struct {
	nbdRequest
}

func (ni *NbdInstance) NewIntegrityTest(t *testing.T, testData []byte) *IntegrityTest {
	b64Decode := base64.NewDecoder(base64.StdEncoding, bytes.NewReader(testData))
	gzipDecode, _ := gzip.NewReader(b64Decode)
	it := &IntegrityTest{
		inflight:    make(map[uint64]Inflight),
		inflightBlk: make(map[uint64]bool),
		lastHandle:  make(map[uint64]uint64),
		quit:        make(chan struct{}),
		tickerQuit:  make(chan struct{}),
		send:        make(chan nbdRequest, 1000000),
		errs:        make(chan error, 4),
		ni:          ni,
		commands:    bufio.NewReaderSize(gzipDecode, 65536),
		statStart:   time.Now(),
	}
	it.inflightCond = sync.NewCond(&it.inflightMutex)
	return it
}

func (it *IntegrityTest) Close() {

}

func (it *IntegrityTest) Abort(err error) {
	it.abortedMutex.Lock()
	if !it.aborted {
		close(it.quit)
		it.aborted = true
		it.ni.CloseConnection()
	}
	if err != nil {
		it.errs <- err // channel should be large enough for all threads
	}
	it.abortedMutex.Unlock()
}

func (it *IntegrityTest) Reader() {
	defer it.wg.Done()
	defer close(it.send)
	// defer fmt.Fprintf(os.Stderr, ">>>> Reader quitting\n")

	for {
		if peek, err := it.commands.Peek(4); err != nil {
			if err == io.EOF {
				return
			}
			it.Abort(fmt.Errorf("Could not peek command source: %v", err))
			return
		} else {
			// the peeked bytes should be one of the command magics
			// unwrap them manually :-(
			magic := (uint64(peek[0]) << 24) | (uint64(peek[1]) << 16) | (uint64(peek[2]) << 8) | uint64(peek[3])

			switch magic {
			case NBD_REQUEST_MAGIC:
				var cmd nbdRequest
				if err := binary.Read(it.commands, binary.BigEndian, &cmd); err != nil {
					if err == io.EOF {
						return
					}
					it.Abort(fmt.Errorf("Could not read command source: %v", err))
					return
				}
				if cmd.NbdRequestMagic != NBD_REQUEST_MAGIC {
					it.Abort(fmt.Errorf("Bad request magic in command source %x", cmd.NbdRequestMagic))
					return
				}
				cmd.NbdOffset &= ^uint64(BLOCKSIZE - 1)
				cmd.NbdLength &= ^uint32(BLOCKSIZE - 1)
				cmd.NbdHandle = getHandle()
				if it.ni.transmissionFlags&NBD_FLAG_SEND_FUA == 0 {
					cmd.NbdCommandFlags &= ^NBD_CMD_FLAG_FUA
				}
				if CmdTypeMap[int(cmd.NbdCommandType)]&CMDT_CHECK_LENGTH_OFFSET == 0 || cmd.NbdLength > 0 {
					atomic.AddUint64(&it.statRead, 1)
					select {
					case it.send <- cmd:
					case <-it.quit:
						return
					}
				}
			case NBD_REPLY_MAGIC:
				var rep nbdReply
				if err := binary.Read(it.commands, binary.BigEndian, &rep); err != nil {
					if err == io.EOF {
						return
					}
					it.Abort(fmt.Errorf("Could not read command source: %v", err))
					return
					if rep.NbdReplyMagic != NBD_REPLY_MAGIC {
						it.Abort(fmt.Errorf("Bad reply magic in command source %x", rep.NbdReplyMagic))
						return
					}
				}
				select {
				case <-it.quit:
					return
				default:
				}
			default:
				it.Abort(fmt.Errorf("Unknown magic: %x", magic))
				return
			}
		}
	}
}

func (it *IntegrityTest) Sender() {
	defer it.wg.Done()
	// defer fmt.Fprintf(os.Stderr, ">>> Sender quitting\n")
	for {
		select {
		case <-it.quit:
			return
		case cmd, ok := <-it.send:
			if !ok {
				return // nothing more to send so reader has closed channel
			}
			it.inflightMutex.Lock()
			blocked := false
			for {
				if _, ok := it.inflight[cmd.NbdHandle]; ok {
					it.inflightMutex.Unlock()
					it.Abort(fmt.Errorf("Command with handle %08x is already in flight", cmd.NbdHandle))
					return
				}
				collision := false
				for addr := cmd.NbdOffset; addr < cmd.NbdOffset+uint64(cmd.NbdLength); addr += BLOCKSIZE {
					if it.inflightBlk[addr] {
						collision = true
						break
					}
				}
				if !collision {
					for addr := cmd.NbdOffset; addr < cmd.NbdOffset+uint64(cmd.NbdLength); addr += BLOCKSIZE {
						it.inflightBlk[addr] = true
					}
					break
				}
				if !blocked {
					blocked = true
					atomic.AddUint64(&it.statBlocked, 1)
				}
				select {
				case <-it.quit:
					it.inflightMutex.Unlock()
					return
				default:
				}
				it.inflightCond.Wait()
			}
			it.inflight[cmd.NbdHandle] = Inflight{nbdRequest: cmd}
			if cmd.NbdCommandType == NBD_CMD_DISC {
				it.disconnectSent = true
			}
			it.inflightMutex.Unlock()
			if err := binary.Write(it.ni.conn, binary.BigEndian, cmd); err != nil {
				it.Abort(fmt.Errorf("Bad write to connection: %v", err))
				return
			}
			if cmd.NbdCommandType == NBD_CMD_WRITE {
				for addr := cmd.NbdOffset; addr < cmd.NbdOffset+uint64(cmd.NbdLength); addr += BLOCKSIZE {
					it.inflightMutex.Lock()
					it.lastHandle[addr] = cmd.NbdHandle
					it.inflightMutex.Unlock()
					data := it.makeData(cmd.NbdHandle, addr)
					if _, err := it.ni.conn.Write(data); err != nil {
						it.Abort(fmt.Errorf("Bad write of data to connection: %v", err))
						return
					}
				}
			}
			atomic.AddUint64(&it.statSent, 1)
		}
	}
}

func (it *IntegrityTest) makeData(handle uint64, addr uint64) []byte {
	data := make([]byte, BLOCKSIZE, BLOCKSIZE)
	if handle == 0 {
		return data
	}
	x := addr ^ (handle << 32) ^ (handle >> 32)
	var i uint64
	for i = 0; i < BLOCKSIZE; i += 8 {
		for j := 0; j < 8; j++ {
			data[i+uint64(j)] = byte((x >> (uint(j * 8))) & 0xff)
		}
		x += uint64(0xFEEDA1ECDEADBEEF) + (i >> 3) + (i << 53)
		s := x & 63
		x = x ^ (x << s) ^ (x >> (64 - s)) ^ uint64(0xAA55AA55AA55AA55) ^ handle
	}
	return data
}

func (it *IntegrityTest) checkData(handle uint64, addr uint64, data []byte) (bool, uint64, uint64) {
	target := it.makeData(handle, addr)
	if bytes.Compare(data, target) == 0 {
		return true, handle, addr
	}
	guessAddr := uint64(data[0]) | (uint64(data[1]) << 8) | (uint64(data[2]) << 16) | (uint64(data[3]) << 24)
	guessHandle := uint64(data[4]) | (uint64(data[5]) << 8) | (uint64(data[6]) << 16) | (uint64(data[7]) << 24)
	return false, guessHandle, guessAddr
}

func (it *IntegrityTest) Receiver() {
	defer it.rxwg.Done()
	// defer fmt.Fprintf(os.Stderr, ">>>> Receiver quitting\n")
	for {
		select {
		case <-it.quit:
			return
		default:
		}
		var rep nbdReply
		var data = make([]byte, 0)
		if err := binary.Read(it.ni.conn, binary.BigEndian, &rep); err != nil {
			if err == io.EOF {
				it.inflightMutex.Lock()
				disconnectSent := it.disconnectSent
				it.inflightMutex.Unlock()
				if disconnectSent {
					atomic.AddUint64(&it.statRecv, 1) // fake a reply
					return
				}
			}
			select {
			case <-it.quit:
				return
			default:
			}
			it.Abort(fmt.Errorf("Bad read from connection: %v", err))
			return
		}
		if rep.NbdReplyMagic != NBD_REPLY_MAGIC {
			it.Abort(fmt.Errorf("Bad magic from connection"))
			return
		}
		it.inflightMutex.Lock()
		cmd, ok := it.inflight[rep.NbdHandle]
		if !ok {
			it.inflightMutex.Unlock()
			it.Abort(fmt.Errorf("Unexpected handle on reply"))
			return
		}
		it.inflightMutex.Unlock()
		if cmd.NbdCommandType == NBD_CMD_READ {
			data = make([]byte, cmd.NbdLength, cmd.NbdLength)
			if _, err := io.ReadFull(it.ni.conn, data); err != nil {
				it.Abort(fmt.Errorf("Bad read of data from connection: %v", err))
				return
			}
			var a uint64
			for addr := cmd.NbdOffset; addr < cmd.NbdOffset+uint64(cmd.NbdLength); addr += BLOCKSIZE {
				it.inflightMutex.Lock()
				handle := it.lastHandle[addr]
				it.inflightMutex.Unlock()
				if ok, guessHandle, guessAddr := it.checkData(handle, addr, data[a:a+BLOCKSIZE]); !ok {
					it.Abort(fmt.Errorf("Corrupt data read handle=%08x expecting handle=%08x addr=%08x. At a guess I got handle=%08x addr=%08x",
						cmd.NbdHandle, handle, addr,
						guessHandle, guessAddr))
					return
				}
				a += BLOCKSIZE
			}
		}
		it.inflightMutex.Lock()
		atomic.AddUint64(&it.statBytes, uint64(cmd.NbdLength))
		atomic.AddUint64(&it.statRecv, 1)
		for addr := cmd.NbdOffset; addr < cmd.NbdOffset+uint64(cmd.NbdLength); addr += BLOCKSIZE {
			delete(it.inflightBlk, addr)
		}
		delete(it.inflight, rep.NbdHandle)
		it.inflightCond.Broadcast()
		it.inflightMutex.Unlock()
	}
}

func (it *IntegrityTest) Stats() string {
	duration := time.Since(it.statStart)
	if duration < time.Microsecond {
		duration = time.Microsecond
	}
	mbps := float64(atomic.LoadUint64(&it.statBytes)) / 1024.0 / 1024.0 / float64(duration) * float64(time.Second)
	return fmt.Sprintf("read=%08d, sent=%08d, recv=%08d blocked=%08d speed=%.3fMBps",
		atomic.LoadUint64(&it.statRead),
		atomic.LoadUint64(&it.statSent),
		atomic.LoadUint64(&it.statRecv),
		atomic.LoadUint64(&it.statBlocked),
		mbps)
}

// prevent rare deadlocks and print the stats out
func (it *IntegrityTest) Ticker() {
	defer it.rxwg.Done()
	// defer fmt.Fprintf(os.Stderr, ">>>> Ticker quitting\n")
	tty := isatty.IsTerminal(os.Stderr.Fd())
	count := 0
	for {
		select {
		case <-it.tickerQuit:
			if tty {
				fmt.Fprintf(os.Stderr, "Complete:    %s      \r", it.Stats())
			}
			return
		case <-time.After(100 * time.Millisecond):
			if count%5 == 0 && tty {
				fmt.Fprintf(os.Stderr, "In progress: %s      \r", it.Stats())
			}
			it.inflightMutex.Lock()
			it.inflightCond.Broadcast()
			it.inflightMutex.Unlock()
		}
		count++
	}
}

func (it *IntegrityTest) Run() error {
	it.wg.Add(2)
	go it.Reader()
	go it.Sender()
	it.rxwg.Add(2)
	go it.Receiver()
	go it.Ticker()
	it.wg.Wait() // wait until everything is sent or the connection is aborted
	// now wait until everything is received
	// fmt.Fprintf(os.Stderr, ">>>> Reader/Sender quit wg clear\n")
	it.inflightMutex.Lock()
	// fmt.Fprintf(os.Stderr, ">>>> Waiting for all packets received or quit signalled\n")
waitloop:
	for {
		it.inflightCond.Wait()
		if atomic.LoadUint64(&it.statRead) == atomic.LoadUint64(&it.statRecv) {
			break waitloop
		}
		select {
		case <-it.quit:
			break waitloop
		default:
		}
	}
	// fmt.Fprintf(os.Stderr, ">>>> All packets received or quit signalled\n")
	it.inflightMutex.Unlock()
	it.Abort(nil)
	close(it.tickerQuit)
	// fmt.Fprintf(os.Stderr, ">>>> Waiting for ticker and receiver to die\n")
	it.rxwg.Wait()
	// fmt.Fprintf(os.Stderr, ">>>> Done\n")

	stats := it.Stats()

errs:
	for {
		select {
		case err := <-it.errs:
			it.ni.t.Logf("Integrity test failed: %s", err)
			it.ni.t.Fail()
		default:
			break errs
		}
	}

	it.ni.t.Logf(stats)

	return nil
}
