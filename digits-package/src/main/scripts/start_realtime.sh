#!/usr/local/bin/bash

COMPONENT_NAME=druid_realtime
APPLICATION_ARGS="server realtime"
USER=${greyhawk_edigits_druid__headlessUser}
KEYDB_FILE=${greyhawk_edigits_druid__keydbFile}

# put broker config in class path
CLASSPATH=$YINST_ROOT/conf/${YINST_VAR_basePkg}/realtime

JVM_ARGS="-Xmx${greyhawk_edigits_druid__realtimeMaxMem} ${greyhawk_edigits_druid__realtimeJavaOpts} -Duser.timezone=UTC -Dfile.encoding=UTF-8 -Djava.io.tmpdir=$YINST_ROOT/tmp/$YINST_VAR_basePkg ${JVM_ARGS}"

. ${YINST_ROOT}/share/${YINST_VAR_basePkg}/scripts/yjava_daemon_start.sh

exit $?