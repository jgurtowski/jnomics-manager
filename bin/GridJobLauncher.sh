#!/bin/bash
#SCRIPT_PATH=/bluearc/home/schatz/sramakri/latest/jnomics-manager/safe_bin
SCRIPT_PATH=`dirname "$0"`
WORKING_DIR=`echo $1 | cut -f1 -d:`
export LD_LIBRARY_PATH=${SCRIPT_PATH}/../lib/lib64
export PATH=$SGE_O_HOME/$WORKING_DIR:$TMPDIR:$PATH
echo $PATH
classp=`find ${SCRIPT_PATH}/../lib/*.jar ${SCRIPT_PATH}/../dist/*.jar | awk '{ s=s$i":"} END{print s}'`
#echo $classp

#if [ $# -ne 0 ] ; 
if [[ "$1" == ":"* ]] ;
then 
	dir=$TMPDIR
	#echo " else is $dir"
else	
	dir=$WORKING_DIR
        #echo " dir is $1"
        if [ ! -d "$dir" ]; then
                mkdir "$dir"
        fi
fi
cd ${dir}
java -cp ${SCRIPT_PATH}/../conf:${classp} edu.cshl.schatz.jnomics.tools.GridJobMain $*
