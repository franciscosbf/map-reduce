#!/bin/sh

#
# basic map-reduce test
#

RACE=

# uncomment this to run the tests with the Go race detector.
#RACE=-race

# run the test in a fresh sub-directory.
rm -rf plugins
mkdir plugins || exit 1
rm -rf bins
mkdir bins || exit 1
rm -rf mr-tmp
mkdir mr-tmp || exit 1

# make sure software is freshly built.
(cd plugins && go build $RACE -buildmode=plugin ../internal/mrapps/wc.go) || exit 1
(cd plugins && go build $RACE -buildmode=plugin ../internal/mrapps/indexer.go) || exit 1
(cd plugins && go build $RACE -buildmode=plugin ../internal/mrapps/mtiming.go) || exit 1
(cd plugins && go build $RACE -buildmode=plugin ../internal/mrapps/rtiming.go) || exit 1
(cd plugins && go build $RACE -buildmode=plugin ../internal/mrapps/crash.go) || exit 1
(cd plugins && go build $RACE -buildmode=plugin ../internal/mrapps/nocrash.go) || exit 1
(cd bins && go build $RACE -o master ../cmd/master/main.go) || exit 1
(cd bins && go build $RACE -o worker ../cmd/worker/main.go) || exit 1
(cd bins && go build $RACE -o sequential ../cmd/sequential/main.go) || exit 1

cd mr-tmp || exit 1

failed_any=0

# first word-count

# generate the correct output
../bins/sequential ../plugins/wc.so ../input/pg*txt || exit 1
sort mr-out-0 >mr-correct-wc.txt
rm -f mr-out*

echo '***' Starting wc test.

timeout -k 2s 180s ../bins/master ../input/pg*txt &

# give the master time to create the sockets.
sleep 1

# start multiple workers.
timeout -k 2s 180s ../bins/worker ../plugins/wc.so &
timeout -k 2s 180s ../bins/worker ../plugins/wc.so &
timeout -k 2s 180s ../bins/worker ../plugins/wc.so &

# wait for one of the processes to exit.
# under bash, this waits for all processes,
# including the master.
wait

# the master or a worker has exited. since workers are required
# to exit when a job is completely finished, and not before,
# that means the job has finished.

sort mr-out* | grep . >mr-wc-all
if cmp mr-wc-all mr-correct-wc.txt; then
	echo '---' wc test: PASS
else
	echo '---' wc output is not the same as mr-correct-wc.txt
	echo '---' wc test: FAIL
	failed_any=1
fi

# wait for remaining workers and master to exit.
wait
wait
wait

# now indexer
rm -f mr-*

# generate the correct output
../bins/sequential ../plugins/indexer.so ../input/pg*txt || exit 1
sort mr-out-0 >mr-correct-indexer.txt
rm -f mr-out*

echo '***' Starting indexer test.

timeout -k 2s 180s ../bins/master ../input/pg*txt &
sleep 1

# start multiple workers
timeout -k 2s 180s ../bins/worker ../plugins/indexer.so &
timeout -k 2s 180s ../bins/worker ../plugins/indexer.so

sort mr-out* | grep . >mr-indexer-all
if cmp mr-indexer-all mr-correct-indexer.txt; then
	echo '---' indexer test: PASS
else
	echo '---' indexer output is not the same as mr-correct-indexer.txt
	echo '---' indexer test: FAIL
	failed_any=1
fi

wait
wait

echo '***' Starting map parallelism test.

rm -f mr-out* mr-worker*

timeout -k 2s 180s ../bins/master ../input/pg*txt &
sleep 1

timeout -k 2s 180s ../bins/worker ../plugins/mtiming.so &
timeout -k 2s 180s ../bins/worker ../plugins/mtiming.so

NT=$(cat mr-out* | grep '^times-' | wc -l | sed 's/ //g')
if [ "$NT" != "2" ]; then
	echo '---' saw "$NT" workers rather than 2
	echo '---' map parallelism test: FAIL
	failed_any=1
fi

if cat mr-out* | grep '^parallel.* 2' >/dev/null; then
	echo '---' map parallelism test: PASS
else
	echo '---' map workers did not run in parallel
	echo '---' map parallelism test: FAIL
	failed_any=1
fi

wait
wait

echo '***' Starting reduce parallelism test.

rm -f mr-out* mr-worker*

timeout -k 2s 180s ../bins/master ../input/pg*txt &
sleep 1

timeout -k 2s 180s ../bins/worker ../plugins/rtiming.so &
timeout -k 2s 180s ../bins/worker ../plugins/rtiming.so

NT=$(cat mr-out* | grep '^[a-z] 2' | wc -l | sed 's/ //g')
if [ "$NT" -lt "2" ]; then
	echo '---' too few parallel reduces.
	echo '---' reduce parallelism test: FAIL
	failed_any=1
else
	echo '---' reduce parallelism test: PASS
fi

wait
wait

# generate the correct output
../bins/sequential ../plugins/nocrash.so ../input/pg*txt || exit 1
sort mr-out-0 >mr-correct-crash.txt
rm -f mr-out*

echo '***' Starting crash test.

rm -f mr-done
(
	timeout -k 2s 180s ../bins/master ../input/pg*txt
	touch mr-done
) &
sleep 1

# start multiple workers
timeout -k 2s 180s ../bins/worker ../plugins/crash.so &

# mimic rpc.go's masterSock()
SOCKNAME=/var/tmp/824-mr-$(id -u)

(while [ -e $SOCKNAME -a ! -f mr-done ]; do
	timeout -k 2s 180s ../bins/worker ../plugins/crash.so
	sleep 1
done) &

(while [ -e $SOCKNAME -a ! -f mr-done ]; do
	timeout -k 2s 180s ../bins/worker ../plugins/crash.so
	sleep 1
done) &

while [ -e $SOCKNAME -a ! -f mr-done ]; do
	timeout -k 2s 180s ../bins/worker ../plugins/crash.so
	sleep 1
done

wait
wait
wait

rm $SOCKNAME
sort mr-out* | grep . >mr-crash-all
if cmp mr-crash-all mr-correct-crash.txt; then
	echo '---' crash test: PASS
else
	echo '---' crash output is not the same as mr-correct-crash.txt
	echo '---' crash test: FAIL
	failed_any=1
fi

if [ $failed_any -eq 0 ]; then
	echo '***' PASSED ALL TESTS
else
	echo '***' FAILED SOME TESTS
	exit 1
fi
