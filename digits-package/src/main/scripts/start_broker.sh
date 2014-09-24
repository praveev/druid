#!/usr/local/bin/bash

COMPONENT_NAME=druid_broker
APPLICATION_ARGS="server broker"
USER=${greyhawk_edigits_druid__headlessUser}
KEYDB_FILE=${greyhawk_edigits_druid__keydbFile}

# put broker config in class path
CLASSPATH=$YINST_ROOT/conf/${YINST_VAR_basePkg}/broker

JVM_ARGS=" -Xmx${greyhawk_edigits_druid__brokerMaxMem} ${greyhawk_edigits_druid__brokerJavaOpts} -Duser.timezone=UTC -Dfile.encoding=UTF-8 -Djava.io.tmpdir=$YINST_ROOT/tmp/$YINST_VAR_basePkg ${JVM_ARGS}"

if [ -n "$greyhawk_edigits_druid__brokerDirectMem" ]; then
    JVM_ARGS = "-XX:MaxDirectMemorySize=${greyhawk_edigits_druid__brokerDirectMem} $JVM_ARGS"
fi

. ${YINST_ROOT}/share/${YINST_VAR_basePkg}/scripts/yjava_daemon_start.sh

exit $?