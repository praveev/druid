#!/usr/local/bin/bash
#
# Script to start yjava_daemon, this is a trimmed down version of the example given in yjava_daemon docs at
# http://twiki.corp.yahoo.com/view/JavaPlatform/PackageYJavaDaemon
#
# This script expects following variables to be defined
# COMPONENT_NAME : name of the component
# JVM_ARGS : jvm arguments for the java process
# APPLICATION_ARGS : arguments to main
# USER : user to start the process with
#

APPLICATION_MAIN_CLASS=com.yahoo.druid.DruidDaemon

PID_FILE=${YINST_ROOT}/var/run/${COMPONENT_NAME}.pid
OUTFILE=${YINST_ROOT}/logs/${YINST_VAR_basePkg}/${COMPONENT_NAME}.out
ERRFILE=${YINST_ROOT}/logs/${YINST_VAR_basePkg}/${COMPONENT_NAME}.err
YJAVA_DAEMON_OUTFILE=${YINST_ROOT}/logs/${YINST_VAR_basePkg}/${COMPONENT_NAME}_start.log

# include base package , hadoop, yjava_daemon jar in the classpath
CLASSPATH=${CLASSPATH}:$(JARS=("$YINST_ROOT/libexec/$YINST_VAR_basePkg"/*.jar); IFS=:; echo "${JARS[*]}")
CLASSPATH=${CLASSPATH}:$YINST_ROOT/conf/hadoop
CLASSPATH=${CLASSPATH}:$(JARS=($YINST_ROOT/share/hadoop/share/hadoop/common/lib/*.jar); IFS=:; echo "${JARS[*]}")
CLASSPATH=${CLASSPATH}:$(JARS=($YINST_ROOT/share/hadoop/share/hadoop/common/*.jar); IFS=:; echo "${JARS[*]}")
CLASSPATH=${CLASSPATH}:$YINST_ROOT/share/hadoop/share/hadoop/hdfs
CLASSPATH=${CLASSPATH}:$(JARS=($YINST_ROOT/share/hadoop/share/hadoop/hdfs/lib/*.jar); IFS=:; echo "${JARS[*]}")
CLASSPATH=${CLASSPATH}:$(JARS=($YINST_ROOT/share/hadoop/share/hadoop/hdfs/*.jar); IFS=:; echo "${JARS[*]}")
CLASSPATH=${CLASSPATH}:$(JARS=($YINST_ROOT/share/hadoop/share/hadoop/yarn/lib/*.jar); IFS=:; echo "${JARS[*]}")
CLASSPATH=${CLASSPATH}:$(JARS=($YINST_ROOT/share/hadoop/share/hadoop/yarn/*.jar); IFS=:; echo "${JARS[*]}")
CLASSPATH=${CLASSPATH}:$(JARS=($YINST_ROOT/share/hadoop/share/hadoop/mapreduce/lib/*.jar); IFS=:; echo "${JARS[*]}")
CLASSPATH=${CLASSPATH}:$(JARS=($YINST_ROOT/share/hadoop/share/hadoop/mapreduce/*.jar); IFS=:; echo "${JARS[*]}")
CLASSPATH=${CLASSPATH}:$YINST_ROOT/lib/jars/yjava_daemon.jar

if [ -e $PID_FILE ]; then
    FILE_PID=`cat $PID_FILE`
    OS_PID=`ps -p $FILE_PID -o pid=`

    if [ "x$OS_PID" = "x" ]; then
        OS_PID=-1
    fi

    if [ $OS_PID = $FILE_PID ]; then
        echo "server is already running"
        exit 0
    else
        echo "removing orphaned $PID_FILE file"
        rm $PID_FILE
    fi
fi


# Setup JAVA_HOME, and java.library.path

if [ "x${yjava_jdk__JAVA_HOME}" != "x" ]; then
    JAVA_HOME="${yjava_jdk__JAVA_HOME}"
else
    JAVA_HOME=${ROOT}/share/yjava_jdk/java/
fi

export JAVA_HOME

# Determine whether we're using the 32 or 64bit JVM (you can use 32 bit JVM on 64bit OS)

MULTIMODELIBEXEC=`${ROOT}/bin/yahoo-mode-choice -e yjava_jdk`


if [ "${MULTIMODELIBEXEC}" == "${ROOT}/libexec64" ]; then
    USE_64BIT_JVM=true
    export USE_64BIT_JVM
    ARCH_LIBEXEC_DIR=${ROOT}/libexec64
    ARCH_LIB_DIR=${ROOT}/lib64
    ARCH_BIN_DIR=${ROOT}/bin64
    echo "$0 ARCH = 64bit"
else
    ARCH_LIBEXEC_DIR=${ROOT}/libexec
    ARCH_LIB_DIR=${ROOT}/lib
    ARCH_BIN_DIR=${ROOT}/bin
    echo "$0 ARCH = 32bit"
fi

# Run as user 'nobody' by default
if [ "x${USER}" == "x" ]; then
    USER=nobody
fi

export JAVA_HOME
echo "$0 JAVA_HOME = ${JAVA_HOME}"

if [ "x${PRELOAD}" != "x" ]; then
  PRELOAD="${ARCH_LIBEXEC_DIR}/yjava_daemon_preload.so ${PRELOAD}"
else
  PRELOAD="${ARCH_LIBEXEC_DIR}/yjava_daemon_preload.so"
fi

START_CMD="env LD_PRELOAD=${PRELOAD} ${ARCH_BIN_DIR}/yjava_daemon -jvm server -pidfile ${PID_FILE} -ynet FILTER_YAHOO -procs 1 -user ${USER} -outfile ${OUTFILE} -errfile ${ERRFILE} -cp ${CLASSPATH} -home ${JAVA_HOME} -Djava.library.path=${ARCH_LIB_DIR} ${JVM_ARGS}  ${APPLICATION_MAIN_CLASS} ${APPLICATION_ARGS}"
echo ${START_CMD} | tee -a ${YJAVA_DAEMON_OUTFILE}
eval "${START_CMD} >> ${YJAVA_DAEMON_OUTFILE} 2>&1"
