#!/usr/local/bin/bash

COMPONENT_NAME=druid_historical
APPLICATION_ARGS="server historical"
USER=${greyhawk_edigits_druid__headlessUser}

# put broker config in class path
CLASSPATH=$YINST_ROOT/conf/${YINST_VAR_basePkg}/historical

JVM_ARGS="-XX:MaxDirectMemorySize=${greyhawk_edigits_druid__historicalDirectMem}  -Xmx${greyhawk_edigits_druid__historicalMaxMem} ${greyhawk_edigits_druid__historicalJavaOpts} -Duser.timezone=UTC -Dfile.encoding=UTF-8 -Djava.io.tmpdir=$YINST_ROOT/tmp/$YINST_VAR_basePkg ${JVM_ARGS}"

. yjava_daemon_start.sh

exit $?