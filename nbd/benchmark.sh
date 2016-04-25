#!/bin/bash
for test in TestConnectionIntegrity TestAioConnectionIntegrity TestConnectionIntegrityHuge TestAioConnectionIntegrityHuge ; do
    for i in 1 2 3 4 5 ; do
	sp=`./nbd.test --test.timeout 6000m -test.run '^'$test'$' -test.v -longtests 2>&1 | grep "integrity_test.go:.* read=" | awk '{print $6}' | awk -F= '{print $2}' | sed -e 's/MBps/\tMBps/'`
	echo -e "$test\t$sp"
    done
done
