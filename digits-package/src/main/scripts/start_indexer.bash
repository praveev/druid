#!/usr/local/bin/bash

ROOT=${ROOT:-$YINST_ROOT}
user=${edigits_druid__headlessUser}

COMPONENT_NAME=digits_druid_Indexer
APPLICATION_MAIN_CLASS=com.yahoo.digits.druid.daemon.IndexerDaemon

#This tells the startup script to use restart configuration
USE_MAXRESTARTS=true
MAXRESTARTS=${MAXRESTARTS:-5}

#If application up for 2 hours, then reset restart counter
# This number should be larger than the interval between runs and the total run time.
# Setting to 2xthe interval to be safe
SECS_UNTIL_STABLE=${SECS_UNTIL_STABLE:-7200}

LIB=$YINST_ROOT/libexec/$YINST_VAR_basePkg

CLASSPATH=${CLASSPATH}:$(JARS=("$LIB"/*.jar); IFS=:; echo "${JARS[*]}")
#digits jar
CLASSPATH=${CLASSPATH}:$YINST_ROOT/conf/${YINST_VAR_basePkg}/indexer
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

source ${ROOT}/share/${YINST_VAR_basePkg}/scripts/yjava_lib.bash
set_yjava_start_globals

# Default 18 threads on prod host result in 8% of time application was stopped.
# 4 threads brings down this to under 2%.
JVM_ARGS="-Xmx${edigits_druid__indexerMaxMem} ${edigits_druid__indexerJavaOpts} -Duser.timezone=UTC -Dfile.encoding=UTF-8 -Djava.io.tmpdir=$YINST_ROOT/tmp/$YINST_VAR_basePkg ${JVM_ARGS}"

standard_start
exit $?