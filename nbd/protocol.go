package nbd

import ()

/* --- START OF NBD PROTOCOL SECTION --- */

// this section is in essence a transcription of the protocol from
// NBD's proto.md; note that that file is *not* GPL. For details of
// what the options mean, see proto.md

// NBD commands
const (
	NBD_CMD_READ         = 0
	NBD_CMD_WRITE        = 1
	NBD_CMD_DISC         = 2
	NBD_CMD_FLUSH        = 3
	NBD_CMD_TRIM         = 4
	NBD_CMD_WRITE_ZEROES = 5
	NBD_CMD_CLOSE        = 7
)

// NBD command flags
const (
	NBD_CMD_FLAG_FUA = uint16(1 << 0)
	NBD_CMD_MAY_TRIM = uint16(1 << 1)
	NBD_CMD_FLAG_DF  = uint16(1 << 2)
)

// NBD negotiation flags
const (
	NBD_FLAG_HAS_FLAGS         = uint16(1 << 0)
	NBD_FLAG_READ_ONLY         = uint16(1 << 1)
	NBD_FLAG_SEND_FLUSH        = uint16(1 << 2)
	NBD_FLAG_SEND_FUA          = uint16(1 << 3)
	NBD_FLAG_ROTATIONAL        = uint16(1 << 4)
	NBD_FLAG_SEND_TRIM         = uint16(1 << 5)
	NBD_FLAG_SEND_WRITE_ZEROES = uint16(1 << 6)
	NBD_FLAG_SEND_DF           = uint16(1 << 7)
	NBD_FLAG_SEND_CLOSE        = uint16(1 << 8)
)

// NBD magic numbers
const (
	NBD_MAGIC                  = 0x4e42444d41474943
	NBD_REQUEST_MAGIC          = 0x25609513
	NBD_REPLY_MAGIC            = 0x67446698
	NBD_CLISERV_MAGIC          = 0x00420281861253
	NBD_OPTS_MAGIC             = 0x49484156454F5054
	NBD_REP_MAGIC              = 0x3e889045565a9
	NBD_STRUCTURED_REPLY_MAGIC = 0x668e33ef
)

// NBD default port
const (
	NBD_DEFAULT_PORT = 10809
)

// NBD options
const (
	NBD_OPT_EXPORT_NAME      = 1
	NBD_OPT_ABORT            = 2
	NBD_OPT_LIST             = 3
	NBD_OPT_PEEK_EXPORT      = 4
	NBD_OPT_STARTTLS         = 5
	NBD_OPT_INFO             = 6
	NBD_OPT_GO               = 7
	NBD_OPT_STRUCTURED_REPLY = 8
)

// NBD option reply types
const (
	NBD_REP_ACK                 = uint32(1)
	NBD_REP_SERVER              = uint32(2)
	NBD_REP_INFO                = uint32(3)
	NBD_REP_FLAG_ERROR          = uint32(1 << 31)
	NBD_REP_ERR_UNSUP           = uint32(1 | NBD_REP_FLAG_ERROR)
	NBD_REP_ERR_POLICY          = uint32(2 | NBD_REP_FLAG_ERROR)
	NBD_REP_ERR_INVALID         = uint32(3 | NBD_REP_FLAG_ERROR)
	NBD_REP_ERR_PLATFORM        = uint32(4 | NBD_REP_FLAG_ERROR)
	NBD_REP_ERR_TLS_REQD        = uint32(5 | NBD_REP_FLAG_ERROR)
	NBD_REP_ERR_UNKNOWN         = uint32(6 | NBD_REP_FLAG_ERROR)
	NBD_REP_ERR_SHUTDOWN        = uint32(7 | NBD_REP_FLAG_ERROR)
	NBD_REP_ERR_BLOCK_SIZE_REQD = uint32(8 | NBD_REP_FLAG_ERROR)
)

// NBD reply flags
const (
	NBD_REPLY_FLAG_DONE = 1 << 0
)

// NBD reply types
const (
	NBD_REPLY_TYPE_NONE         = 0
	NBD_REPLY_TYPE_ERROR        = 1
	NBD_REPLY_TYPE_ERROR_OFFSET = 2
	NBD_REPLY_TYPE_OFFSET_DATA  = 3
	NBD_REPLY_TYPE_OFFSET_HOLE  = 4
)

// NBD hanshake flags
const (
	NBD_FLAG_FIXED_NEWSTYLE = 1 << 0
	NBD_FLAG_NO_ZEROES      = 1 << 1
)

// NBD client flags
const (
	NBD_FLAG_C_FIXED_NEWSTYLE = 1 << 0
	NBD_FLAG_C_NO_ZEROES      = 1 << 1
)

// NBD errors
const (
	NBD_EPERM     = 1
	NBD_EIO       = 5
	NBD_ENOMEM    = 12
	NBD_EINVAL    = 22
	NBD_ENOSPC    = 28
	NBD_EOVERFLOW = 75
)

// NBD info types
const (
	NBD_INFO_EXPORT      = 0
	NBD_INFO_NAME        = 1
	NBD_INFO_DESCRIPTION = 2
	NBD_INFO_BLOCK_SIZE  = 3
)

// NBD new style header
type nbdNewStyleHeader struct {
	NbdMagic       uint64
	NbdOptsMagic   uint64
	NbdGlobalFlags uint16
}

// NBD client flags
type nbdClientFlags struct {
	NbdClientFlags uint32
}

// NBD client options
type nbdClientOpt struct {
	NbdOptMagic uint64
	NbdOptId    uint32
	NbdOptLen   uint32
}

// NBD export details
type nbdExportDetails struct {
	NbdExportSize  uint64
	NbdExportFlags uint16
}

// NBD option reply
type nbdOptReply struct {
	NbdOptReplyMagic  uint64
	NbdOptId          uint32
	NbdOptReplyType   uint32
	NbdOptReplyLength uint32
}

// NBD request
type nbdRequest struct {
	NbdRequestMagic uint32
	NbdCommandFlags uint16
	NbdCommandType  uint16
	NbdHandle       uint64
	NbdOffset       uint64
	NbdLength       uint32
}

// NBD simple reply
type nbdReply struct {
	NbdReplyMagic uint32
	NbdError      uint32
	NbdHandle     uint64
}

// NBD info export
type nbdInfoExport struct {
	NbdInfoType          uint16
	NbdExportSize        uint64
	NbdTransmissionFlags uint16
}

// NBD info blocksize
type nbdInfoBlockSize struct {
	NbdInfoType           uint16
	NbdMinimumBlockSize   uint32
	NbdPreferredBlockSize uint32
	NbdMaximumBlockSize   uint32
}

/* --- END OF NBD PROTOCOL SECTION --- */

// Our internal flags to characterize commands
const (
	CMDT_CHECK_LENGTH_OFFSET     = 1 << iota // length and offset must be valid
	CMDT_REQ_PAYLOAD                         // request carries a payload
	CMDT_REQ_FAKE_PAYLOAD                    // request does not carry a payload, but we'll make a zero payload up
	CMDT_REP_PAYLOAD                         // reply carries a payload
	CMDT_CHECK_NOT_READ_ONLY                 // not valid on read-only media
	CMDT_SET_DISCONNECT_RECEIVED             // a disconnect - don't process any further commands
)

// A map specifying each command
var CmdTypeMap = map[int]uint64{
	NBD_CMD_READ:         CMDT_CHECK_LENGTH_OFFSET | CMDT_REP_PAYLOAD,
	NBD_CMD_WRITE:        CMDT_CHECK_LENGTH_OFFSET | CMDT_CHECK_NOT_READ_ONLY | CMDT_REQ_PAYLOAD,
	NBD_CMD_DISC:         CMDT_SET_DISCONNECT_RECEIVED,
	NBD_CMD_FLUSH:        CMDT_CHECK_NOT_READ_ONLY,
	NBD_CMD_TRIM:         CMDT_CHECK_LENGTH_OFFSET | CMDT_CHECK_NOT_READ_ONLY,
	NBD_CMD_WRITE_ZEROES: CMDT_CHECK_LENGTH_OFFSET | CMDT_CHECK_NOT_READ_ONLY | CMDT_REQ_FAKE_PAYLOAD,
	NBD_CMD_CLOSE:        CMDT_SET_DISCONNECT_RECEIVED,
}
