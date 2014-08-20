#!/usr/local/bin/bash
#
#    This script starts wraps common logic for platform yjava components
#    Copyright (c) 2012-2013 Yahoo! Inc. All rights reserved.

READY_WAIT_RETRIES=${READY_WAIT_RETRIES:-120}
HALT_CODE=${HALT_CODE:--1}
REQUIRE_HALT=${REQUIRE_HALT:-false}

function set_yjava_start_globals() {
    PID_FILE=${ROOT}/var/run/${COMPONENT_NAME}.pid
    OUTFILE=${ROOT}/logs/${basePkg}/${COMPONENT_NAME}.out
    ERRFILE=${ROOT}/logs/${basePkg}/${COMPONENT_NAME}.err
    TIME_STAMP=`date +"%Y-%m-%d_%H-%M"`
    USE_MAXRESTARTS=${USE_MAXRESTARTS:-true}
    PROFILINGFILE=${ROOT}/logs/${basePkg}/${COMPONENT_NAME}.hprof.${TIME_STAMP}
    GCLOGFILE=${ROOT}/logs/${basePkg}/${COMPONENT_NAME}.gc.${TIME_STAMP}


    # Setup JAVA_HOME, and java.library.path
    JAVA_HOME=${yjava_jdk__JAVA_HOME:-${ROOT}/share/yjava_jdk/java/}
    export JAVA_HOME

    # Determine whether we're using the 32 or 64bit JVM (you can use 32 bit JVM on 64bit OS)

    MULTIMODELIBEXEC=`${ROOT}/bin/yahoo-mode-choice -e yjava_jdk`

    if [[ ${MULTIMODELIBEXEC} == "${ROOT}/libexec64" ]]; then
        USE_64BIT_JVM=true
        export USE_64BIT_JVM
        ARCH_LIBEXEC_DIR=${ROOT}/libexec64
        ARCH_LIB_DIR=${ROOT}/lib64
        ARCH_BIN_DIR=${ROOT}/bin64
    else
        ARCH_LIBEXEC_DIR=${ROOT}/libexec
        ARCH_LIB_DIR=${ROOT}/lib
        ARCH_BIN_DIR=${ROOT}/bin
    fi

    # Run as user 'nobody' by default, else use yinst setting.    Change ownership of files used at runtime to match user
    USER=${user:-nobody}

    if [ "${debug}" == "true" ]; then
        YJAVA_DAEMON_DEBUG="-debug"
    else
        YJAVA_DAEMON_DEBUG=""
    fi
    YNET_FILTER=${ynet_filter:-FILTER_YAHOO}

    PRELOAD="${ARCH_LIBEXEC_DIR}/yjava_daemon_preload.so:${PRELOAD}"
    JVM_ARGS="-Djava.library.path=${ARCH_LIB_DIR} ${JVM_ARGS}"

    PROFILING=${profiling:-false}
    if [ "$PROFILING" == "true" ]; then
        JVM_ARGS="${JVM_ARGS} -agentlib:hprof=cpu=samples,depth=12,interval=5,thread=y,doe=y,file=${PROFILINGFILE}"
    fi

    LOG_GC=${log_gc:-false}
    if [ "$LOG_GC" == "true" ]; then
        JVM_ARGS="${JVM_ARGS} -XX:+PrintTenuringDistribution"
        JVM_ARGS="${JVM_ARGS} -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:${GCLOGFILE}"
        JVM_ARGS="${JVM_ARGS} -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=15 -XX:GCLogFileSize=5M"
    fi

}

function set_yjava_stop_globals() {
    PID_FILE=${ROOT}/var/run/${COMPONENT_NAME}.pid
    ALARMTIME=${ALARMTIME:-120}
}


#Return zero if there is no pidfile
#Return pid of pidfile if there is one and there is a valid process matching it
#Remove any pidfile which doesn't have a matching process
function confirm_pid() {
    local PID_FILE=$1

    if [[ -n $SCRIPT_DEBUG ]]; then
        echo "Checking for PID file: ${PID_FILE}" 
    fi

    if [[ -e $PID_FILE ]]; then
        local FILE_PID=$(cat $PID_FILE)
        if [[ -n $SCRIPT_DEBUG ]]; then
            echo "PID of $COMPONENT_NAME: $FILE_PID" 
        fi

        local OS_PID=$(ps -p $FILE_PID -o pid:1=)

        #Is pidfile orphaned?    If so, then remove it.
        if [[ -z $OS_PID ]]; then
            echo "removing orphaned ${COMPONENT_NAME}.pid file"
            rm $PID_FILE        
        fi
    fi

    #export to global PID because pid return values are too large for $?
    if [[ -e $PID_FILE ]]; then
        PID=$(cat $PID_FILE)
    else
        PID=0
    fi
}

function check_up_during_start()
{
    local PID_FILE=$1
    confirm_pid $PID_FILE
    if [[ $PID -ne 0 ]]; then
        echo "${COMPONENT_NAME} is already running"
        #The component already being up isn't considered an
        # error because a common case will be recovering from partial application downage
        exit 0
    fi
}

function check_up_during_stop()
{
    local PID_FILE=$1
    confirm_pid $PID_FILE
    if [[ $PID -eq 0 ]]; then
        echo "${COMPONENT_NAME} is not running"
        # If the component is not running, there's nothing to stop
        exit 0
    fi
}

function confirm_component_started() {
    #Wait for up to 1 minute after start script completed for ready file to appear
    local TRIES=0
    while [[ ($TRIES -lt $READY_WAIT_RETRIES) ]]; do
        sleep 0.5
        TRIES=$[TRIES+1]
    done
}

function standard_start() {
    START=$(date +%s.%N)
    check_up_during_start $PID_FILE
    RESTART_ENV=""
    if $USE_MAXRESTARTS ; then
        RESTART_ENV="-maxrestarts ${MAXRESTARTS:-5} -secsuntilstable ${SECS_UNTIL_STABLE:-60}"
    fi

    START_COMMAND="LD_PRELOAD=${PRELOAD} JAVA_HOME=${JAVA_HOME} ${ARCH_BIN_DIR}/yjava_daemon -jvm server $RESTART_ENV -pidfile ${PID_FILE} ${YJAVA_DAEMON_DEBUG} -ynet ${YNET_FILTER} -procs ${PROCESSES:-1} -user ${USER} -outfile ${OUTFILE} -errfile ${ERRFILE} -home ${JAVA_HOME} -cp ${CLASSPATH} ${JVM_ARGS} ${APPLICATION_MAIN_CLASS} ${APPLICATION_ARGS}"
    if [[ -n $SCRIPT_DEBUG ]]; then
        echo "Executing start command: ${START_COMMAND}"
    fi
    eval "${START_COMMAND} >> /home/y/logs/${basePkg}/${COMPONENT_NAME}_start.log"
    RESULT=$?
    if [ $RESULT -eq 0 ] ; then
        echo "$COMPONENT_NAME successfully started."
    else
        echo "$COMPONENT_NAME had startup errors."
    fi

    END=$(date +%s.%N)
    DIFF=$(echo "$END - $START" | bc)
    echo "Time to start: $DIFF"
    return $RESULT
}

#Kill a component process with SIGINT
function soft_kill() {
    local PID=$1
    kill    $PID
    echo "$COMPONENT_NAME shutting down cleanly."
}

#Kill a component process with SIGTERM along with all its' child processes
function hard_kill() {
    local PID=$1
    PIDS=`pgrep -P $PID`

    kill -s SIGKILL $PID $PIDS
    echo "$COMPONENT_NAME forced to stop after timeout."
}

function confirm_component_stopped() {
    local PID_FILE=$1
    MAX_TRIES=80
    #Wait for up to 40 seconds after stop script completed for ps process to disappear
    local TRIES=0
    while [[ ($TRIES -lt $MAX_TRIES) && ( -d /proc/$PID ) && ( -z `grep zombie /proc/$PID/status` ) ]]; do
        sleep 0.5
        TRIES=$[TRIES+1]
    done
    if [[ $TRIES -eq $MAX_TRIES ]]; then
        echo "Exhausted all tries waiting for pid to close: $PID."
    fi
    if [[ -z `ps -p $PID -o pid:1=` ]]; then
        echo "Successfully stopped ${COMPONENT_NAME}"
        #Cleanup the PID file only after stop is confirmed
        if [[ -e $PID_FILE ]]; then
            rm $PID_FILE;
        fi
        return 0
    else
        echo "An error occured while stopping ${COMPONENT_NAME} "
        if $REQUIRE_HALT; then
            return $HALT_CODE
        else
            return 1
        fi
    fi
}

function kill_with_timeout() {    
    local PID_FILE=$1
    local TIMEOUT=$2
    local CURRENT_PROCESS_PID=$$
    local COMPONENT_PID=$PID

     #Attempt waiting for TIMEOUT seconds, try to force kill the component. Signal the main thread when done.
     ( sleep $TIMEOUT; hard_kill $COMPONENT_PID $COMPONENT_NAME; \
                    echo "Hard killing $COMPONENT_NAME."; kill -s SIGALRM $CURRENT_PROCESS_PID ) &
     local HARD_STOP_PID=$!

    #Attempt immediately to do a clean stop
    ( soft_kill $COMPONENT_PID $PID_FILE ) &

    #Block until the clean stop thread pid disappears either due to successful completion 
    # or to having the underlying PID cleared out
     while [ -e /proc/$COMPONENT_PID ]; do sleep 0.1; done

    disown $HARD_STOP_PID 2>/dev/null
    kill $HARD_STOP_PID 2>/dev/null
    confirm_component_stopped $PID_FILE
}

function standard_stop() {
    START=$(date +%s.%N)
    check_up_during_stop $PID_FILE
    kill_with_timeout $PID_FILE $ALARMTIME

    RESULT=$?
    END=$(date +%s.%N)
    DIFF=$(echo "$END - $START" | bc)
    echo "Time to stop: $DIFF"
    return $RESULT   
}

