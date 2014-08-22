#!/usr/local/bin/bash

#
# Kill the daemon.
# See twiki.corp.yahoo.com/view/JavaPlatform/PackageYJavaDaemon for more info
# Usage: yjava_daemon_stop.sh <component_name>
#

PID_FILE=${YINST_ROOT}/var/run/${1}.pid
echo "Killing the daemon for pid file ${PID_FILE}"

# Stop app only if its running
if [ -e ${PID_FILE} ]; then
    FILE_PID=`cat ${PID_FILE}`
    OS_PID=`ps -p $FILE_PID -o pid=`

    if [ "x$OS_PID" = "x" ]; then
        OS_PID=-1
    fi

    if [ $OS_PID != $FILE_PID ]; then
        echo "daemon is not running, removing orphaned ${PID_FILE} file"
        rm $PID_FILE
        exit 0
    fi

    kill $FILE_PID
else
    echo "daemon is not running"
    exit 0
fi

# Wait for the app process to exit before returning.

OS_PID=$FILE_PID

until [ $OS_PID != $FILE_PID ]; do
    sleep 1
    OS_PID=`ps -p $FILE_PID -o pid=`

    if [ "x$OS_PID" = "x" ]; then
        OS_PID=-1
    fi
done

rm ${PID_FILE}

exit 0