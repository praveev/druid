#!/usr/local/bin/bash

COMPONENT_NAME=druid_coordinator
APPLICATION_ARGS="server coordinator"
USER=${greyhawk_edigits_druid__headlessUser}
KEYDB_FILE=${greyhawk_edigits_druid__keydbFile}

# put broker config in class path
CLASSPATH=$YINST_ROOT/conf/${YINST_VAR_basePkg}/coordinator

JVM_ARGS="-Xmx${greyhawk_edigits_druid__coordinatorMaxMem} ${greyhawk_edigits_druid__coordinatorJavaOpts} -Duser.timezone=UTC -Dfile.encoding=UTF-8 -Djava.io.tmpdir=$YINST_ROOT/tmp/$YINST_VAR_basePkg ${JVM_ARGS}"

. ${YINST_ROOT}/share/${YINST_VAR_basePkg}/scripts/yjava_daemon_start.sh

exit $?