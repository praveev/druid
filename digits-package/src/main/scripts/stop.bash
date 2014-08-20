#!/usr/local/bin/bash

ROOT=${ROOT:-$YINST_ROOT}
user=${edigits_druid__headlessUser}

stop(){	
	
   if [ -z "$1" ]                           # Is parameter #1 zero length?
   then
     echo "-Parameter #1 is zero length.-"  # Or no parameter passed.
   fi
   
   if [ -z "$2" ]                           # Is parameter #2 zero length?
   then
     echo "-Parameter #2 is zero length.-"  # Or no parameter passed.
   fi
   
   COMPONENT_NAME=$1
   APPLICATION_MAIN_CLASS=$2
   
   echo "${COMPONENT_NAME}:${APPLICATION_MAIN_CLASS}"
	
	source ${ROOT}/share/${YINST_VAR_basePkg}/scripts/yjava_lib.bash
	set_yjava_stop_globals
	
	standard_stop
}

stop $1 $2

exit $?