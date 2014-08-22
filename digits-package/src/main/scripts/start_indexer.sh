#!/usr/local/bin/bash

COMPONENT_NAME=druid_indexer
APPLICATION_ARGS="server overlord"
USER=${greyhawk_edigits_druid__headlessUser}

# put broker config in class path
CLASSPATH=$YINST_ROOT/conf/${YINST_VAR_basePkg}/indexer

JVM_ARGS="-Xmx${greyhawk_edigits_druid__indexerMaxMem} ${greyhawk_edigits_druid__indexerJavaOpts} -Duser.timezone=UTC -Dfile.encoding=UTF-8 -Djava.io.tmpdir=$YINST_ROOT/tmp/$YINST_VAR_basePkg ${JVM_ARGS}"

. yjava_daemon_start.sh

exit $?