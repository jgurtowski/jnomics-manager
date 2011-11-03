#!/bin/bash

############################################################################
## 
##  Hydra pipeline script. Schatz Lab, Cold Spring Harbor Labs, 2011 
## 
##  Greatly extended by Matthew Titmus (mtitmus@cshl.edu) and Michael Schatz
##  (mschatz@cshl.edu) from a version originally written by Aaron Quinlan; it 
##  is available at http://code.google.com/p/hydra-sv/wiki/TypicalWorkflow
##
##  Revision: 847
##  Date: Wed Sep 28 16:53:40 EDT 2011
##
############################################################################


###[ Pipeline component directories ########################################
##
## Directories for dependent components may be specified here. 

# Location of the jar file used by this script.
JAR_FILE=$PWD/jnomics.jar

# The Hadoop directory
HADOOP_DIR=

# The Hydra package, which provides the primary functions of this pipeline. 
# Available for download at http://code.google.com/p/hydra-sv.
HYDRA_DIR=

# If blank, this will default to $HYDRA_DIR/../scripts
HYDRA_SCRIPTS_DIR=

# The directory that contains the Novoalign tool binary. Available from 
# Novocraft (http://www.novocraft.com)
#NOVOCRAFT_DIR=/bluearc/data/schatz/software/packages/novocraft-2.07.06
NOVOCRAFT_DIR=

# Contains the Python scripts used in tier 3. 
#SCRIPTS_DIR=$PWD/jnomics/scripts/
SCRIPTS_DIR=


###[ Command parameter properties ]#########################################

# The location of the BAM file. If the $SAM_FILE doesn't exist, it is 
# generated from this.
BAM_FILE=

# The prefix of the BWA index. 
BWA_PREFIX=

# The FASTA-formatted sequence file. If a novoalign index doesn't exist, this
# will be used to generate one. 
FASTA=

# The location of the FASTQ read pair files.
FQ1_FILE=
FQ2_FILE=

# The default working directory in the distributed filesystem.
# Default: /user/<username>
FS_DIR=

# The pre-indexed Novoalign database file.
NOVO_INDEX=

# The number of threads to use for sequence alignment. If not specified, the 
# aligners (BWA and/or Novoalign) will use their defaults.
ALIGN_THREADS=

# The location of the SAM file. If this is blank, then it is required to be
# defined in the command line parameters.
SAM_FILE=


# The approximate fragment length and standard deviation used by -i novoalign
# parameter (mode is always 'PE' for our purposes). If not specified, these
# are determined automatically.
FRAG_LEN_AVG=
FRAG_LEN_STDEV=

# Fragment m.a.d. See: http://en.wikipedia.org/wiki/Median_absolute_deviation). 
# TODO Determine these during the read processing.
FRAG_LEN_MAD=

# Maximum allowable length difference b/w mappings. Typically set to 
# 10 * m.a.d. of the DNA fragment libraries. Blanking makes it required at the
# command line. See: http://en.wikipedia.org/wiki/Median_absolute_deviation). 
HYDRA_MLD=

# Maximum allowable non-overlap b/w mappings. Typically set to 
# median + (20 * m.a.d.) of the DNA fragment libraries. Blanking this makes 
# it required at the command line.
HYDRA_MNO=

# Maximum size to judge a read as 'concordant'. Default=$FRAG_LEN_AVG + 3 * $MAD
MAX_CONCORDANT_RANGE=

# Minimum fragment length. Sequences shorter than this (after trimming) will 
# be discarded. 0 = No mininum. Blanking this makes it required in the 
# command line.
FASTQ_MIN_SEQ_LEN=0

# Quality threshold: nucleotides with lower quality will be trimmed 
# (from the end of the sequence). 0 = No trimming. Blanking this makes it 
# required at the command line.
FASTQ_QUAL_THRESHOLD=0

# Default value of -q flag. "on"=suppress output. Currently incompletely 
# supported.
QUIET="off"

## Hadoop task timeout values (in milliseconds)
# Standard timeout value (equal to Hadoop default). 10 minutes.
TIMEOUT_NORMAL=$[10 * 60 * 1000]

# 16 times normal (2 hour 40 minutes)
TIMEOUT_LONG=$[16 * $TIMEOUT_NORMAL]

# 64 times normal (10 hours 40 minutes)
TIMEOUT_VLONG=$[64 * $TIMEOUT_NORMAL]

# The number of nodes in the local cluser. Determined automatically.
HADOOP_NODE_COUNT=


###[ Miscellaneous properties ]#############################################

# The name of the log file this script appends.
LOG_FILE=runHydra.log


###[ Exit codes used by this script. Do not change ]########################

# For thoroughness. Successful/errorless exits use this value.
E_OK=0

# Failure related to contents of parameters
E_BAD_PARAM=5

# An external process exited with a non-zero code. 
E_EXTERNAL=7

# Internal error (no fault of the user or pipeline components)
E_INTERNAL=11


###[ Do not change ]########################################################

# The number of reduce tasks to use. Gets set to 0.9 * $HADOOP_NODE_COUNT 
# 	* ${mapred.tasktracker.reduce.tasks.maximum} 
HADOOP_MAPRED_REDUCE_TASKS=

# Default hadoop options.
HADOOP_OPTIONS=

# A standard Hadoop configuration file for all Hadoop processes. 
HADOOP_CONF=

# The Phred offset value of the dataset, which is automatically 
# converted to 33 (SAM standard).
PHRED_OFFSET_IN=33


############################################################################
##  Script Functions
############################################################################


############################################################################
## FUNCTION: calculateSizeStats()
## 
## If $FRAG_LEN_AVG and $FRAG_LEN_STDEV are undefined, this scans the first 
## ~10000 reads in $SAM_FILE and calculates the template length mean and
## standard deviation.

calculateSizeStats() {
	if [ -z "$FRAG_LEN_AVG" ] || [ -z "$FRAG_LEN_STDEV" ]
	then
		if [ -e "$SCRIPTS_DIR/samStats.py" ]
		then
			SAMSTATS_DIR=$SCRIPTS_DIR
		else
			SAMSTATS_DIR=$(fullyResolveDir samStats.py)
			checkStatus
		fi

		if [ "$SAM_EXISTS" != "true" ]
		then
			echo "calculateSizeStats: \$SAM_FILE undefined or does not exist (\"$SAM_FILE\")" >&2
			exit $E_EXTERNAL
		fi
		
		if [ -z "$SAMSTATS_DIR" ] || [ ! -e "$SAMSTATS_DIR" ]
		then
			echo "$(basename $0): Cannot find samStats.py in scripts directory or on the command path." >&2
			exit $E_EXTERNAL
		fi

		sizeStats=$($HADOOP_DIR/hadoop fs $HADOOP_CONF $HADOOP_OPTIONS -cat $SAM_FILE | $SAMSTATS_DIR/samStats.py -i stdin -n 10000 -r 0)
		checkStatus

		if [ -z "$FRAG_LEN_AVG" ]; then
			FRAG_LEN_AVG=$(echo "$sizeStats" | cut -f 1)
		fi

		if [ -z "$FRAG_LEN_STDEV" ]; then
			FRAG_LEN_STDEV=$(echo "$sizeStats" | cut -f 2)
		fi
	fi

	if [ -z "$FRAG_LEN_MAD" ]; then
		FRAG_LEN_MAD=$[$[$FRAG_LEN_STDEV * 10000] / 14826]
	fi

	if [ -z "$HYDRA_MLD" ]; then
		HYDRA_MLD=$[$FRAG_LEN_MAD * 10]
	fi

	if [ -z "$HYDRA_MNO" ]; then
		HYDRA_MNO=$[$FRAG_LEN_MAD * 20]
	fi

	if [ -z "$MAX_CONCORDANT_RANGE" ]; then
		MAX_CONCORDANT_RANGE=$[$FRAG_LEN_AVG + $[$FRAG_LEN_MAD * 3]]
	fi
}


############################################################################
## FUNCTION: checkStatus
##
## Checks the return code of the most recently executed command, and halts 
## the script if any non-zero value is found.

checkStatus() {
	status=$?
	lineNumber=

	if [ "$1" == "-L" ]
	then
		lineNumber=$2
	fi

	if [ ! -z "$lineNumber" ]
	then
		lineNumber="Line $lineNumber: "
	fi

	if [[ ! $status -eq 0 ]]
	then
		if [[ $status -eq 126 ]]
		then
			msg="Command cannot execute (permissions problem?)"
		elif [[ $status -eq 127 ]]
		then
			msg="Command not found"
		elif [[ $status -gt 128 ]]
		then
			msg="Signal $(($status-128))"

			case "$(($status-128))" in
				"1" )
					msg="$msg: Hangup detected on controlling terminal or death of controlling process"
					;;
				"2" )
					msg="$msg: Interrupt from keyboard"
					;;
				"3" )
					msg="$msg: Quit from keyboard"
					;;
				"4" )
					msg="$msg: Illegal Instruction"
					;;
				"6" )
					msg="$msg: Abort signal from abort(3)"
					;;
				"8" )
					msg="$msg: Floating point exception"
					;;
				"9" )
					msg="$msg: Kill signal"
					;;
				"11" )
					msg="$msg: Invalid memory reference"
					;;
				"13" )
					msg="$msg: Broken pipe: write to pipe with no readers"
					;;
				"14" )
					msg="$msg: Timer signal from alarm(2)"
					;;
				"15" )
					msg="$msg: Termination signal"	
					;;
		  		# Failure condition.
				* )
					msg="Got non-zero exit code from external process"
				;;
			esac
		else
			msg="Got non-zero exit code from external process"
		fi

		doExit $E_EXTERNAL "$lineNumber$msg (code=$status)"
	fi 

	return 0
}

alias checkStatus='checkStatus -L $LINENO'
shopt -s expand_aliases


############################################################################
## FUNCTION: killForks (deprecated)
##
## Called when the scripts exits to kill any active subprocesses. 
## Specifically, it kills all processes with a PPID equal to this script's 
## PID with a "forked but didn’t exec" flag, and a CMD of $(basename $0).

killForks() {
	# If there are any active subprocesses, kill them. Specifically, kill all
	# processes with a PPID equal to this script's PID, with a "forked but
	# didn’t exec" flag, and a CMD of $(basename $0).

	for child in $(ps -l --ppid=$$ | grep "^1 .* $(basename $0)\$" | awk '{print $4}')
	do
		kill $child >> /dev/null 2>&1
	done
}


############################################################################
## FUNCTION: out
## SYNOPSIS: out <message>
##
## Simple output function. In addition to outputting to standard out, it
## also appends a log file. In both cases it preprends the amount of time 
## transpired since the start of the script.

startTimeInSeconds=$(date +%s)

out() {
	text=""
		
	if [ ! -z "$1" ]
	then
		text="$@"
	fi 
    
	sec=$(date +%s)
	(( sec-=startTimeInSeconds ))
	
	h=$(( $sec/3600 ))
	(( sec%=3600 ))

	min=$(( $sec/60 ))
	(( sec%=60 ))

	TIMESTAMP=$(printf '%02dh %02dm %02ds' $h $min $sec)
	if [ ! $QUIET == "on" ]
	then
		echo -e "[$TIMESTAMP] $text"
	fi 
	
	echo -e "[$TIMESTAMP] $text" >> $LOG_FILE
	
	return 0
}


############################################################################
## FUNCTION: syntax()
## SYNOPSIS: Dumps the script command line syntax to stdout.

syntax() {
	echo "Usage: $(basename $0) (-n|-f) <index-file> -mld <bp> -mno <bp> [OPTION]" 
	echo
	echo "  -dir <dir>     REQUIRED. The HDFS file system working directory."
	echo "  -f <db.fasta>  REQUIRED. The FASTA-formatted sequence file."
	echo "  -n <file>      REQUIRED (OR -f). The pre-indexed Novoalign database file." 
	echo "  -p <prefix>    Prefix of the BWA index. Default=FASTA file name."
	echo "  -jar <file>    The Jnomics JAR file. Default=$JAR_FILE"
	echo 
	echo "Short-read input options (ANY ONE REQUIRED):"
	echo "  -fq <fq> <fq>  Fastq read pair files."
	echo "  -sam <file>    Path of an aligned SAM file. Default=tier1.sam"
	echo "  -bam <file>    Path of an aligned BAM file. Default=tier1.bam" 
	#echo "  -C             Complementation: reverse all reads prior to tier 1 alignment." 
	echo
	echo "Read processing and filtering options:"
	echo "  -l <len>       Minimum fragment length. Sequences shorter than this (after " 
	echo "                 trimming) will be discarded. 0 = No min. Default=$FASTQ_MIN_SEQ_LEN."
	echo "  -Q <int>       Handle ASCII quality strings with the specified offset. "
	echo "                 Typical values are 33 or 64. Default: $PHRED_OFFSET_IN."
	echo "  -t <phred>     Quality threshold: nucleotides with lower quality will be " 
	echo "                 trimmed (from the end of the sequence). Default=$FASTQ_QUAL_THRESHOLD."
	echo "  -x <int>       The minimum genomic distance that a readpair must span in " 
	echo "                 order to be reported, not counting the read lengths (i.e., " 
	echo "                 inner span, not fragment size). Default: 100."
	echo "  -y <int>       The minimum size to judge a readpair as 'concordant' with " 
	echo "                 respect to the reference genome. Assumes fragment libraries " 
	echo "                 not matepair libraries (+/- orientation is concordant). " 
	echo "                 Default: 0."
	echo "  -z <int>       Maximum read size to judge as concordant. Default=$MAX_CONCORDANT_RANGE."
	echo
	echo "Short read alignment options:"
	echo "  -c             Number of threads to use for read alignments. Default: Aligner default."
	echo   
	echo "Hydra options:"	
	echo "  -mld <bp>      Maximum allowable length difference b/w mappings. Typically " 
	echo "                 set to 10 * m.a.d. of the DNA fragment libraries." 
	echo "                 See: http://en.wikipedia.org/wiki/Median_absolute_deviation"
	echo "  -mno <bp>      Maximum allowable non-overlap b/w mappings. Typically set to " 
	echo "                 median + (20 * m.a.d.) of the DNA fragment libraries."
	echo "  -ms <pairs>    Minimum number of pairs required for variant to be called. " 
	echo "                 Default: 2"
	echo "  -lnk  <bp>     Maximum intrachromosomal distance allowed before a variant is " 
	echo "                 considered to be between unlinked DNA segments. " 
	echo "                 Default: 1000000 (i.e., 1Mb)"
	echo "  -is            Choose most likely variant (when a tie exists) based on least " 
	echo "                 edit distance rather than size."
	echo "  -li            Combine +/+ and -/- mappings when screening for inversions. " 
	echo "                 This increases sensitivity in low coverage."
	echo "  -use           Which mappings should be used for each pair?"
	echo "                   'best'  Use the mappings with the least edit distance"
	echo "                           for each pair (Default)"
	echo "                   'all'   Use all mappings for each pair."
	echo "                   <INT>   Use the best plus those within <INT> edit"
	echo "                               distance of best."        
	echo 
	echo "Hadoop options:"
	echo "  -conf <file>             Specify a Hadoop configuration file."
	echo "  -D key=value             "
	echo "  -fs <local|host:port>    Specify a Hadoop namenode." 
	echo "  -jt <local|host:port>    Specify a Hadoop job tracker."
	echo 
	echo "Miscellaneous options:"
	echo "  -e <email>     Comma-delimited list of addresses to email when the script " 
	echo "                 completes."
	#echo "  --no-cleanup   Do not remove temporary files."
	echo "  -?, --help     Display this help and exit."
}


############################################################################
## FUNCTION: sendMail - Sends an email on script failure/completion
## SYNOPSIS: sendMail [-s subject] -t to-addr -m "message contents" 
##
## Recipients can be defined by changing the value of EMAIL_DEFAULT_RECIPIENTS
## or using the function parameters.  

EMAIL_DEFAULT_SUBJECT="(No subject)"
EMAIL_DEFAULT_RECIPIENTS=""

sendMail() {	
	SUBJECT=$EMAIL_DEFAULT_SUBJECT
	RECIPIENTS=$EMAIL_DEFAULT_RECIPIENTS
	TMP_PATH="/tmp/emailmessage.txt"
	
	index=1
	while [ $index -le $# ]
	do  
		case "${!index}" in	
			"-s" )
				(( index++ ))
				SUBJECT=${!index}
			;; 
			"-t" )
				(( index++ ))
				RECIPIENTS=${!index}
			;; 
			"-m" )
				(( index++ ))
				MESSAGE=${!index}
			;; 
		
	  		# Failure condition.
			* )
				echo "sendmail syntax error: \"sendMail $*\"" 1>&2
				echo "Syntax: sendMail -s \"subject\" -t \"to-addr\" -m \"message\"" 1>&2
				return 1
			;;
		esac
	
		(( index++ ))
	done
	
	# If there are no recipients, then there's no reason to continue. Since
	# it's considered a valid setting, though, it's not an error.
	 
	if [ ! -z $RECIPIENTS ]
	then	
		if [ -z "$MESSAGE" ]
		then
			echo "EMAIL FAILURE: Message empty" 1>&2
			return 1
		fi
		
		echo -e $MESSAGE > $TMP_PATH	
		/bin/mail -s "$SUBJECT" "$RECIPIENTS" < /tmp/emailmessage.txt
	fi 
	
	return 0
}


#############################################################################
## FUNCTION: doExit()
## SYNOPSIS: doExit <exit status> <message>
##
## Catches script exits (expected or otherwise), and performs any necessary 
## notifications via email or otherwise.

doExit() {
	subject="Hydra pipeline:"
	text=$@
	code=$E_UNKNOWN
	
	if [ ! -z "$(echo $@ | grep '^[0-9]*\( .*\)\?$')" ]
	then
		code=$(echo $@ | awk '{print $1}')
		shift
		text="$@"
	fi

	case "$code" in
		"$E_UNKNOWN" )
			subject="$subject halted for an unknown reason."
		;; 

		"$E_OK" )
			subject="$subject completed successfully!"
		;; 

		"$E_EXTERNAL" )
			subject="$subject failed (error code returned by external process)"
		;; 

		"$E_BAD_PARAM" )
			subject="$subject failed (bad parameters passed to $(basename $0))"
		;; 

		"$E_MISSING_SAVE" )
			subject="$subject failed (saved parameters could not be found)"
		;; 

		"$E_INTERNAL" )
			subject="$subject failed (internal error)"
		;; 

  		# Failure condition.
		* )
			subject="$subject failed (unknown error code $code)"
		;;
	esac

	killForks
	
	if [ -z "$text" ] 
	then
		text="No message given. See runHydra.log for more info."
	fi
	
	out "$text"
	sendMail -s "$subject" -m "$text"

	exit $code
}


############################################################################
## FUNCTION: doHadoopSetup()
##  
## Gathers information for the current Hadoop configuration

doHadoopSetup() {	
	HADOOP_NODE_COUNT=$($HADOOP_DIR/hadoop dfsadmin $HADOOP_CONF \
		$HADOOP_OPTIONS -report | grep "Decommission Status : Normal" | wc -l)
	
	if [ $HADOOP_NODE_COUNT -eq 0 ]
	then
		HADOOP_NODE_COUNT=1
	fi
	
	# TODO Support standard Hadoop options and add $HADOOP_CONF $HADOOP_OPTIONS
	HADOOP_MAPRED_REDUCE_TASKS=$($HADOOP_DIR/hadoop jar $JAR_FILE query mapred.tasktracker.reduce.tasks.maximum)
	
	if [ "$HADOOP_MAPRED_REDUCE_TASKS" == "(null)" ] || [ $HADOOP_MAPRED_REDUCE_TASKS == "0" ]
	then
		HADOOP_MAPRED_REDUCE_TASKS=1
	fi
	
	HADOOP_MAPRED_REDUCE_TASKS=$[ $HADOOP_NODE_COUNT * $HADOOP_MAPRED_REDUCE_TASKS * 9 ]
	HADOOP_MAPRED_REDUCE_TASKS=$[ $HADOOP_MAPRED_REDUCE_TASKS / 10 ]
}


############################################################################
## FUNCTION: fullyResolve() and fullyResolveDir()
## SYNOPSIS: fullyResolve <cmd>
##           fullyResolveDir <cmd>
##
## Given a command on $PATH, these echo the complete path of the binary and
## binary's directory, respectively. All symlinks components of the path are
## recursively canonicalized. Nothing is echoed if the command is not found
## on the user's path.

fullyResolve() {
	WHICHBIN=$(which $@)

	if [ ! -z "$WHICHBIN" ]
	then
		echo $(readlink -f "$WHICHBIN")
	fi
}

fullyResolveDir() {
	WHICHDIR=$(fullyResolve $@)

	if [ ! -z $WHICHDIR ]
	then
		echo $(dirname $WHICHDIR)
	fi
}


############################################################################
## FUNCTION: testHadoopTaskSuccessful()
## SYNOPSIS: testHadoopTaskSuccessful <hdfs-dir>
##  
## Searches a given a directory name for a _SUCCESS file, and sets the 
## _SUCCESS flag to "1" if it finds it, or to an empty value ("") if it
## doesn't. If the directory exists but doesn't have a _SUCCESS file, it
## also will remove the directory.

testHadoopTaskSuccessful() {
	testDir=$@
	_SUCCESS=
		
	if [ -z $testDir ]
	then
		doExit $E_INTERNAL "testHadoopTaskSuccessful: missing HDFS output directory name"
	fi
		
	# Does the directory itself exist? Hadoop -test returns 0 if the file exists.
	
	$HADOOP_DIR/hadoop fs $HADOOP_CONF $HADOOP_OPTIONS -test -e $testDir >> /dev/null 2>&1
	if [ $? -eq 0 ]
	then
		# The directory exists; does the _SUCCESS marker file?
		
		$HADOOP_DIR/hadoop fs $HADOOP_CONF $HADOOP_OPTIONS -test -e $testDir/_SUCCESS >> /dev/null 2>&1
		if [ $? -eq 0 ]
		then
			_SUCCESS=1
		else
			out "Tier 1: $testDir exists but is missing _SUCCESS marker; deleting"
			$HADOOP_DIR/hadoop fs $HADOOP_CONF $HADOOP_OPTIONS -rmr $testDir >> /dev/null 2>&1
		fi
	fi
}


############################################################################
## FUNCTION: verifyNovoIndex()
##
## Determines whether the reference index and/or FASTA files are
## specified/exist. If the FASTA is specified but the index is not, this will
## look for the index as $FASTA.ndx and create it if it doesn't exist. 

## TODO Push the index to the DFS and make it available to the NovoProxy. 

verifyNovoIndex() {
 	if [ -z $NOVO_INDEX ] && [ ! -z $FASTA ]
 	then
		NOVO_INDEX=$FASTA.ndx
 	fi
 	
	if [ ! -e $NOVO_INDEX ]
	then
		out "Tier 2: Novoindexed reference sequence not found: building now. (You're welcome)"
		
		if [ -z $FASTA ] || [ ! -e $FASTA ]
		then
			msg="Reference sequence build failed: can't find FASTA file ($FASTA)." 
			out "Tier 2: $msg"
			doExit $E_BAD_PARAM $msg
		else
			$NOVOCRAFT_DIR/novoindex -k 14 -s 1 -m $NOVO_INDEX $FASTA
			
			# One last check...
			if [ -e $NOVO_INDEX ]
			then
				out "Tier 2: Reference sequence build complete ($NOVO_INDEX)."
			else
				doExit $E_EXTERNAL "Failed to build novo index."
			fi
		fi
	fi
	
	return 0
}


############################################################################
## FUNCTION: verifySamExists()
##
## Is the SAM file in the HDFS? If not, look for the BAM, and if it exists
## stream it to the HDFS, converting to SAM on the fly.

verifySamExists() {
	SAM_EXISTS=
	BAM_EXISTS=
	
	# If no BAM is specified, we will look for a SAM file of the same name. 
	# If it exists we assume they're named identically.
	
	out "Searching for SAM/BAM files:"

	if [ -z "$BAM_FILE" ] && [ -z "$SAM_FILE" ]
	then
		BAM_FILE=$FS_DIR/tier1.bam
		SAM_FILE=$FS_DIR/tier1.sam
	elif [ -z "$SAM_FILE" ]
	then
		# We have a BAM, but no SAM. That's okay.
		SAM_FILE=$FS_DIR/$(basename "$BAM_FILE" | sed 's/.bam/.sam/')
	fi

	# We treat blank BAM's differently (we ignore them).

	if [ -z "$BAM_FILE" ]
	then
		out "   BAM: [\033[1;33mNOT SPECIFIED\033[0m]"
	else	
		# If the BAM file has no URI scheme, convert into a fully-qualified 
		# "file://" URI

		if [ -z $(echo "$BAM_FILE" | grep '^.[a-zA-Z0-9]\+:') ]
		then 
			BAM_FILE=file://$(readlink -f $BAM_FILE)
		fi
		
		# Does the BAM file exist? 

		$HADOOP_DIR/hadoop fs $HADOOP_CONF $HADOOP_OPTIONS -test -e $BAM_FILE >> $LOG_FILE 2>&1
		if [ $? -eq 0 ]
		then
			BAM_EXISTS=true	
		fi 
	
		if [ $BAM_EXISTS ]
		then
			out "   BAM: $BAM_FILE [\033[1;32mFOUND\033[0m]"
		else
			out "   BAM: $BAM_FILE [\033[1;31mNOT FOUND\033[0m]"
		fi 
	fi

	# Use convertcopy to fully resove the SAM file URL
	# TODO Add support for standard Hadoop options
	# SAM_FILE=$($HADOOP_DIR/hadoop jar $JAR_FILE convertcopy $HADOOP_CONF 
	# $HADOOP_OPTIONS -p -o $SAM_FILE | grep 'out=' | sed 's/out=//')
	SAM_FILE=$($HADOOP_DIR/hadoop jar $JAR_FILE convertcopy -p -o $SAM_FILE | grep 'out=' | sed 's/out=//')
	
	# Check for the presence of the SAM and BAM files.
	$HADOOP_DIR/hadoop fs $HADOOP_CONF $HADOOP_OPTIONS -test -e $SAM_FILE >> $LOG_FILE 2>&1
	if [ $? -eq 0 ]
	then
		SAM_EXISTS=true
	fi
	
	if [ $SAM_EXISTS ]
	then
		out "   SAM: $SAM_FILE [\033[1;32mFOUND\033[0m]"
	else
		out "   SAM: $SAM_FILE [\033[1;31mNOT FOUND\033[0m]"
	fi 
	
	# Now that we've settled on file candidates, decide whether to convert, 
	# upload, and/or ignore

	if [ $SAM_EXISTS ]
	then			
		# If the SAM file exists but its URL points to the local filesystem,
		# we upload the file into the HDFS. Otherwise, we have nothing to do.

		if [ -z $(echo $SAM_FILE | grep '^hdfs:') ]
		then
			# The URL doesn't point into the HDFS, but did we upload it to 
			# the HDFS previously?

			SAM_BASE_NAME=$(basename $SAM_FILE)
		
			$HADOOP_DIR/hadoop fs $HADOOP_CONF $HADOOP_OPTIONS \
				-test -e $FS_DIR/$SAM_BASE_NAME >> $LOG_FILE 2>&1

			if [ $? -eq 0 ]
			then
				# Yup - found it!

				out "Found a version of SAM in the HDFS; skipping upload"
			else
				out "SAM exists on local filesystem, but not in HDFS: uploading"
				
				$HADOOP_DIR/hadoop fs $HADOOP_CONF $HADOOP_OPTIONS -put $SAM_FILE $FS_DIR/$SAM_BASE_NAME.tmp
				$HADOOP_DIR/hadoop fs $HADOOP_CONF $HADOOP_OPTIONS -mv $FS_DIR/$SAM_BASE_NAME.tmp $FS_DIR/$SAM_BASE_NAME
				checkStatus
				
				SAM_FILE=$FS_DIR/$SAM_BASE_NAME
			fi			
		fi
	elif [ $BAM_EXISTS ]
	then
		# The BAM exists but SAM does not: convert and put into the HDFS
		# TODO Add a percentage or some other kind of progress indicator?

		out "Streaming/converting local BAM to HDFS"
		
		$HADOOP_DIR/hadoop jar $JAR_FILE convertcopy -i $BAM_FILE -o $SAM_FILE
		checkStatus
		
		# It exists now!
		SAM_EXISTS=true
	fi
}


############################################################################
## Start script body
############################################################################
	 
echo "Hydra pipeline script (Hydra-Hadoop). Revision 847 (Wed Sep 28 16:53:40 EDT 2011)"
echo "Matthew A. Titmus, Cold Spring Harbor Labs, 2011"
echo
	
# No args? Just output the command syntax and exit.
if [ ! -n "$1" ]
then
	syntax
	exit 0
fi

############################################################################
## Handle command line arguments.

# We know we have args. The user must mean business. If there's already a log
# file in this space, pad with a couple of extra lines of readability.

if [ -s $LOG_FILE ]
then
	echo -e "\n" >> $LOG_FILE
fi

# Loops over the args. There's probably a better way to do this, but it 
# works for now.

index=1
while [ $index -le $# ]
do  
	case "${!index}" in	
		### Base options 
		
		"-f" | "--fasta" )
			(( index++ ))
			FASTA=${!index}
		;;
			
		"-dir" )
			(( index++ ))
			FS_DIR=${!index}
		;; 
		
		"-jar" )
			(( index++ ))
			JAR_FILE=${!index}
		;; 
		
		"-n" | "--novo-index" )
			(( index++ ))
			NOVO_INDEX=${!index}
		;; 
		
		"-p" )
			(( index++ ))
			BWA_PREFIX=${!index}
		;;
		

		### Short-read input options (ANY ONE REQUIRED)

		"-fq" )
			(( index++ ))
			FQ1_FILE=${!index}
			(( index++ ))
			FQ2_FILE=${!index}
		;;			
		
		"-bam" )
			(( index++ ))
			BAM_FILE=${!index}
		;;			
			
		"-sam" )
			(( index++ ))
			SAM_FILE=${!index}
		;; 


		### Read processing and filtering options
		
		"-l" | "--fq-seqlen" )
			(( index++ ))
			FASTQ_MIN_SEQ_LEN=${!index}
		;; 

		"-Q" )
			(( index++ ))
			$PHRED_OFFSET_IN=${!index}
		;;
		
		
		"-t" | "--fq-qthresh" )
			(( index++ ))
			FASTQ_QUAL_THRESHOLD=${!index}
		;;
		
		"-x" )
			(( index++ ))
			echo "Warning: option x is not yet supported." >&2
		;;	
		
		"-y" )
			(( index++ ))
			echo "Warning: option y is not yet supported." >&2
		;;	
		
		"-z" )
			(( index++ ))
			MAX_CONCORDANT_RANGE=${!index}
		;;
		

		### Short read alignment options
		
		"-c" | "--align-threads" )
			(( index++ ))
			ALIGN_THREADS=${!index}
		;;
		

		### Hydra options
		
		"-mld" )
			(( index++ ))
			HYDRA_MLD=${!index}
		;; 
		
		"-mno" )
			(( index++ ))
			HYDRA_MNO=${!index}
		;;
		
		"-ms" )
			(( index++ ))
			HYDRA_MS=${!index}
		;;
		
		"-lnk" )
			(( index++ ))
			HYDRA_LNK=${!index}
		;;
		
		"-is" )
			(( index++ ))
			HYDRA_IS=1
		;;
		
		"-li" )
			(( index++ ))
			HYDRA_LI=1
		;;
		
		"-use" )
			(( index++ ))
			HYDRA_USE=${!index}
		;;

		
		### Hadoop options

		"-conf" )
			(( index++ ))
			HADOOP_CONF="${!index}"
		;;

		"-D" )
			(( index++ ))
			HADOOP_OPTIONS="$HADOOP_OPTIONS -D ${!index}"
		;;

		"-fs" )
			(( index++ ))
			HADOOP_OPTIONS="$HADOOP_OPTIONS -fs ${!index}"
		;;

		"-jt" )
			(( index++ ))
			HADOOP_OPTIONS="$HADOOP_OPTIONS -jt ${!index}"
		;;

		
		### Miscellaneous options

		"-e" | "--email" )
			(( index++ ))
			EMAIL_DEFAULT_RECIPIENTS=${!index}
		;;
		
		"--no-cleanup" )
			NO_CLEANUP=1
		;; 
		
		"--scripts" )
			(( index++ ))
			SCRIPTS_DIR=${!index}
		;; 
		
		"-?" | "--help" )
			syntax
			exit 0
		;;


		### Not officially and/or fully supported

		"-q" | "--quiet" )
			(( index++ ))
			QUIET=${!index}
			    
			if [ ! "$QUIET" == "on" ] && [ ! "$QUIET" == "off" ]
			then
				echo "Expected --quiet (on|off); got: --quiet $QUIET" 1>&2
				exit $E_BAD_PARAM
			fi
		;;
	
  		# Failure condition.
		* )
			echo "$(basename $0): invalid option -- ${!index}" 1>&2
			echo "Try '$(basename $0) --help' for more information." 1>&2 
			exit $E_BAD_PARAM
		;;
	esac

	(( index++ ))
done


# If the required paths are not defined, and attempt to resolve them if they're not.

if [ -z "$HADOOP_DIR" ]
then
	HADOOP_DIR=$(fullyResolveDir hadoop)
fi
if [ ! -s "$HADOOP_DIR" ]
then
	echo "$(basename $0): Cannot find Hadoop directory (HADOOP_DIR=$HADOOP_DIR)" >&2
	echo "Verify that the directory exists and this script's HADOOP_DIR property is correctly set" >&2

	exit 1
elif [ ! -d "$HADOOP_DIR" ]
then
	echo "$(basename $0): $HADOOP_DIR is not a directory." >&2
	echo "Verify that the the HADOOP_DIR property is correctly set." >&2
	
	exit 1
fi

if [ -z "$HYDRA_DIR" ]
then
	HYDRA_DIR=$(fullyResolveDir hydra)
fi
if [ ! -e "$HYDRA_DIR" ]
then
	echo "Cannot find Hydra directory: $HYDRA_DIR" >&2
	echo "Please verify that it is correctly defined in the script parameters." >&2
	exit 1
else
	if [ -z "$HYDRA_SCRIPTS_DIR" ]
	then
		HYDRA_SCRIPTS_DIR=$(fullyResolveDir dedupDiscordants.py)
	fi

	if [ -z "$HYDRA_SCRIPTS_DIR" ]
	then
		HYDRA_SCRIPTS_DIR="$(echo "$HYDRA_DIR" | sed 's|/bin/\?$||')/scripts"
	fi

	if [ ! -e "$HYDRA_SCRIPTS_DIR" ]
	then
		echo "Cannot find Hydra scripts directory: $HYDRA_SCRIPTS_DIR" >&2
		echo "Please verify that it is correctly defined in the script parameters." >&2
		exit 1
	fi
fi

if [ -z "$NOVOCRAFT_DIR" ]
then
	NOVOCRAFT_DIR=$(fullyResolveDir novoalign)
fi
if [ ! -s "$NOVOCRAFT_DIR" ]
then
	echo "$(basename $0): Cannot find Novocraft directory (NOVOCRAFT_DIR=$NOVOCRAFT_DIR)" >&2
	echo "Verify that the directory exists and this script's NOVOCRAFT_DIR property is correctly set" >&2
	
	exit 1
elif [ ! -d "$NOVOCRAFT_DIR" ]
then
	echo "$(basename $0): $NOVOCRAFT_DIR is not a directory." >&2
	echo "Verify that the the NOVOCRAFT_DIR property is correctly set." >&2
	
	exit 1
elif [ ! -e "$NOVOCRAFT_DIR/novoindex" ]
then
	echo "$(basename $0): $NOVOCRAFT_DIR doesn't appear to be a valid Novocraft directory" >&2
	echo "Verify that the the NOVOCRAFT_DIR property is correctly set." >&2
	
	exit 1
fi

if [ -z "$SCRIPTS_DIR" ]
then
	SCRIPTS_DIR=$(fullyResolveDir matchpairs.pl)
fi
if [ ! -e "$SCRIPTS_DIR" ]
then
	echo "Cannot find scripts directory: $SCRIPTS_DIR" >&2
	echo "Please verify that it is correctly defined in the script parameters." >&2
	exit 1
fi


############################################################################
# Check for undefined required values; output any warnings/errors to stderr.

if [ ! -s $JAR_FILE ]
then
	echo "$(basename $0): Cannot find core JAR file at $JAR_FILE (JAR_FILE=$JAR_FILE)" >&2
	echo "Verify that the file exists and this script's JAR_FILE property is correctly set" >&2
	exit $E_BAD_PARAM 
fi

if [ -z "$SAM_FILE" ] && [ -z "$BAM_FILE" ] && [ -z "$FQ1_FILE" ] && [ -z "$FQ2_FILE" ]
then
	echo "One or more of the following are required: bam, sam, fq" >&2
	exit $E_BAD_PARAM 
fi

if [ -z "$FASTQ_QUAL_THRESHOLD" ] 
then
	echo "Missing required flag: -t" >&2
	exit $E_BAD_PARAM 
fi

if [ -z $NOVO_INDEX ] && [ -z $FASTA ]
then
	echo "$(basename $0): reference index file not specified either by novoindex (-n) or FASTA files (-f)" >&2
	exit $E_BAD_PARAM 
fi

#if [ -z $HYDRA_MLD ] || [ -z $HYDRA_MLD ] 
#then
#	echo "$(basename $0): Maximum allowable length (-mld) and/or overlap (-mno) not specified." >&2
#	exit $E_BAD_PARAM 
#fi

if [ -z "$FS_DIR" ] 
then
	echo "$(basename $0): HDFS working directory (-dir) not defined." >&2
	exit $E_BAD_PARAM 
else
	# Fully resolve the filesystem working directory 
	FS_DIR=$($HADOOP_DIR/hadoop jar $JAR_FILE convertcopy -p -o $FS_DIR | grep 'out=' | sed 's/out=//')
	checkStatus
fi

# If $HADOOP_CONF is defined, it should exist.
if [ ! -z "$HADOOP_CONF" ] 
then	
	if [ ! -s "$HADOOP_CONF" ]
	then
		echo "$(basename $0): Specified configuration file $HADOOP_CONF does not exist" >&2
		exit $E_BAD_PARAM 
	fi	
fi

if [ ! -z "$HADOOP_CONF" ] 
then	
	HADOOP_CONF="-conf $HADOOP_CONF"
fi

doHadoopSetup

out "Hydra-Hadoop pipeline started: $(date)"

echo " HDFS working directory: $FS_DIR"
echo " Jnomics JAR file:       $JAR_FILE"
echo " Hadoop directory:       $HADOOP_DIR"
echo " Hydra directory:        $HYDRA_DIR"
echo " Scripts directory:      $SCRIPTS_DIR"
echo " Hadoop configuration:   $(echo "$HADOOP_CONF" | sed 's|-conf ||')"
echo " Hadoop node count:      $HADOOP_NODE_COUNT"
echo " Reduce tasks to use:    $HADOOP_MAPRED_REDUCE_TASKS"

echo " HDFS working directory: $FS_DIR" >> $LOG_FILE
echo " Jnomics JAR file:         $JAR_FILE" >> $LOG_FILE
echo " Hadoop directory:       $HADOOP_DIR" >> $LOG_FILE
echo " Hydra directory:        $HYDRA_DIR" >> $LOG_FILE
echo " Scripts directory:      $SCRIPTS_DIR" >> $LOG_FILE
echo " Hadoop configuration:   $(echo "$HADOOP_CONF" | sed 's|-conf ||')" >> $LOG_FILE
echo " Hadoop node count:      $HADOOP_NODE_COUNT" >> $LOG_FILE
echo " Reduce tasks to use:    $HADOOP_MAPRED_REDUCE_TASKS" >> $LOG_FILE


############################################################################
# Determine the number of threads to dedicate to performing alignments. If 
# a number isn't specified, we allow the default values to take over.

BWA_THREADS=
NOVO_THREADS=
		
if [ ! -z "$ALIGN_THREADS" ]
then
	out "Using $ALIGN_THREADS threads for alignment."
	BWA_THREADS="-t $ALIGN_THREADS"
	NOVO_THREADS="-c $ALIGN_THREADS"
fi


############################################################################
## Tier 1 alignment: The focus of this tier is to quickly identify (and remove)
## easily identifiable concordant readpairs from the alignment. To do this, we 
## align the paired-end reads to the reference genome with a fast and 
## reasonably sensitive aligner (in this case, BWA). 

out "***********[ Tier 1 ]***********"
## Does the indicated SAM file exist in the HDFS? 
verifySamExists

if [ $SAM_EXISTS ]
then
	out "Tier 1: $SAM_FILE already exists; skipping tier 1 alignment."
else		
	if [ -z "$FQ1_FILE" ] || [ -z "$FQ2_FILE" ]
	then
		doExit $E_BAD_PARAM "Tier 1: $SAM_FILE was not found and no fastq files were specified (-fq flag)"
	else
		out "Tier 1: $SAM_FILE not found; continuing with tier 1 alignment."
	fi

	## Process the FASTQ files and convert then into flattened FASTQ 
	## (FlASTQ?) format so the BWA process can use them. Also, take this 
	## opportunity to eliminate any low-quality reads.
  
	if [ -z "$BWA_PREFIX" ]
	then
		BWA_PREFIX=$FASTA
	  	bwaAlnArgs="bwa index -a <bwtsw|div|is>"
	else		
	  	bwaAlnArgs="bwa index -a <bwtsw|div|is> -p $BWA_PREFIX"
	fi

	if [ ! -s $BWA_PREFIX.bwt ]
	then
		out "Tier 1: Verifying presence of BWT index: [\033[1;31mNOT FOUND\033[0m]"
	
		bwaAlnArgs="cd $(dirname $FASTA); $bwaAlnArgs $(basename $FASTA)"
		
		out "Could not find BWT index file at expected location ($FASTA.bwt)"
		out "Run: '$bwaAlnArgs'"
		out "using the algorithm appropriate for your data:"
		out "	'-a bwtsw' for long genomes"
		out "	'-a is' or '-a div' for short genomes."
		
		exit $E_BAD_PARAM
	else
		out "Tier 1: Verifying presence of BWT index: [\033[1;32mFOUND\033[0m]"
	fi 
  	
  	testHadoopTaskSuccessful $FS_DIR/tier1_1
  	if [ -z $_SUCCESS ]
  	then
		argsmsg="-Q $PHRED_OFFSET_IN 33 -l $FASTQ_MIN_SEQ_LEN -t $FASTQ_QUAL_THRESHOLD"

		out "Tier 1: Performing pre-alignment read processing ($argsmsg)"
		
		$HADOOP_DIR/hadoop jar $JAR_FILE processor $HADOOP_CONF $HADOOP_OPTIONS \
			-in $FQ1_FILE $FQ2_FILE -out $FS_DIR/tier1_1 \
			-fout pair \
			-Q $PHRED_OFFSET_IN 33 \
			-l $FASTQ_MIN_SEQ_LEN \
			-t $FASTQ_QUAL_THRESHOLD \
			-D mapred.job.name="hydra.tier1.pre.processing" \
			-D mapred.reduce.tasks=$HADOOP_MAPRED_REDUCE_TASKS
		checkStatus
	else
		out "Tier 1: Skipping pre-alignment read processing: already done"
	fi

  	testHadoopTaskSuccessful $FS_DIR/tier1_2
  	if [ -z $_SUCCESS ]
  	then
		out "Tier 1: Performing BWA alignment"
	
		$HADOOP_DIR/hadoop jar $JAR_FILE bwa $HADOOP_CONF $HADOOP_OPTIONS \
			-in $FS_DIR/tier1_1 -out $FS_DIR/tier1_2 \
			-fout sam \
			-db $FASTA \
			-D mapred.job.name="hydra.tier1.bwa.alignment" \
			-D jnomics.map.fail.no-reads.in="false" \
			-D mapred.reduce.tasks=$HADOOP_NODE_COUNT \
			-D mapred.task.timeout=$TIMEOUT_LONG
		checkStatus
	
		$HADOOP_DIR/hadoop fs $HADOOP_CONF $HADOOP_OPTIONS -mv $FS_DIR/tier1_2/part-r-00000.sam $SAM_FILE
		checkStatus

		SAM_EXISTS=true
	fi
fi

calculateSizeStats

out "Tier 1: Got basic read size statistics:"
out "    Average length/stddev:  $FRAG_LEN_AVG / $FRAG_LEN_STDEV"
out "    Median absolute dev:    $FRAG_LEN_MAD"
out "    Max length difference:  $HYDRA_MLD"
out "    Max non-overlap:        $HYDRA_MNO"
out "    Max concordant range:   $MAX_CONCORDANT_RANGE"


# If the tier 1_3 directory already exists, skip this step. Otherwise, process
# the tier 1 reads.

testHadoopTaskSuccessful $FS_DIR/tier1_3
if [ -z $_SUCCESS ]
then
	argsmsg="-Q $PHRED_OFFSET_IN 33 -l $FASTQ_MIN_SEQ_LEN -t $FASTQ_QUAL_THRESHOLD"
	out "Tier 1: Processing aligned reads ($argsmsg)"
	
	# If tier1_data exists, delete it.
	
	$HADOOP_DIR/hadoop fs $HADOOP_CONF $HADOOP_OPTIONS -test -e $FS_DIR/tier1_data >> $LOG_FILE 2>&1
	if [ $? -eq 0 ]
	then
		$HADOOP_DIR/hadoop fs $HADOOP_CONF $HADOOP_OPTIONS -rmr $FS_DIR/tier1_data >> $LOG_FILE 2>&1
	fi

	# Generate read data and download the resulting table 
	
	#$HADOOP_DIR/hadoop jar $JAR_FILE stats $HADOOP_CONF $HADOOP_OPTIONS \
	#	-fin sam -in $SAM_FILE -out $FS_DIR/tier1_data \
	#	-D mapred.job.name="hydra.tier1.readstats"
	#
	#$HADOOP_DIR/hadoop fs $HADOOP_CONF $HADOOP_OPTIONS \
	#	-get $FS_DIR/tier1_data/part-r-00000.sam tier1.insert.sizes
	#
	#head -n 200 tier1.insert.sizes

	# Processes the SAM file(s), and discards fragments in which both parts 
	# are properly aligned (-F 2). Generates one or more part-r-* files
	# that contain the results in "paired read" format.

	$HADOOP_DIR/hadoop jar $JAR_FILE processor $HADOOP_CONF $HADOOP_OPTIONS \
		-in $SAM_FILE -out $FS_DIR/tier1_3 \
		-fout pair \
		-F 2 -Q $PHRED_OFFSET_IN 33 \
		-l $FASTQ_MIN_SEQ_LEN \
		-t $FASTQ_QUAL_THRESHOLD \
		-D mapred.job.name="hydra.tier1.post.processing" \
		-D mapred.reduce.tasks=$HADOOP_MAPRED_REDUCE_TASKS

	checkStatus	
else
	out "Tier 1: Skipping read processing: $FS_DIR/tier1 already exists."
fi


############################################################################
## Tier 2 alignment. Here we grab the discordant reads from the tier 1
## processing results and align them with a more sensitive aligner
## (Novoalign) using a word size of no more than 15bp (we typically use 11-14
## with Novoalign at this stage). This is necessary as many concordant pairs
## are missed in the first tier (a consequence of the tradeoff b/w speed and
## senitivity), which lead to false positive SV calls. At this point, you are
## merely trying to eliminate remaining concordant pairs, thus, we use the
## "-r Random" alignment mode for Novoalign. 

out "***********[ Tier 2 ]***********"

# Perform the more sensitive tier 2 alignment using the processed (and much
# smaller number of) reads. 

testHadoopTaskSuccessful $FS_DIR/tier2_1
if [ -z $_SUCCESS ]
then
	verifyNovoIndex

	out "Tier 2: Checking alignments against tier 1 reads."

	$HADOOP_DIR/hadoop jar $JAR_FILE novoproxy $HADOOP_CONF $HADOOP_OPTIONS \
		-in $FS_DIR/tier1_3 \
		-out $FS_DIR/tier2_1 \
		-novo $NOVOCRAFT_DIR \
		-d $NOVO_INDEX \
		-H -i $FRAG_LEN_AVG $FRAG_LEN_STDEV -r Random -o SAM \
		-fout sam \
		-D mapred.job.name="hydra.tier2.alignment" \
		-D mapred.reduce.tasks=$HADOOP_MAPRED_REDUCE_TASKS \
		-D io.sort.mb=100 \
		-D mapred.child.java.opts=-Xmx300m \
		-D mapred.task.timeout=$TIMEOUT_LONG
	
		checkStatus	
else
	out "Tier 2: Skipping alignment: $FS_DIR/tier2_1 already exists."
fi


## Process the reads again to identify discordant pairs and match the
## resulting pairs

testHadoopTaskSuccessful $FS_DIR/tier2_2
if [ -z $_SUCCESS ]
then
	out "Tier 2: Identifying discordant pairs and matching results."
	
	$HADOOP_DIR/hadoop jar $JAR_FILE processor $HADOOP_CONF $HADOOP_OPTIONS \
		-in $FS_DIR/tier2_1 -out $FS_DIR/tier2_2 \
		-fout pair \
		-F 2 -Q $PHRED_OFFSET_IN 33 \
		-D mapred.reduce.tasks=$HADOOP_MAPRED_REDUCE_TASKS \
		-D mapred.job.name="hydra.tier2.processing"
				
	checkStatus	
else
	out "Tier 2: Skipping alignment: $FS_DIR/tier2_2 already exists."
fi


############################################################################
## Tier 3

out "***********[ Tier 3 ]***********"

testHadoopTaskSuccessful $FS_DIR/tier3_1
if [ -z $_SUCCESS ]
then
	verifyNovoIndex
	
	out "Tier 3: Aligning Tier 2 matches"
	
	$HADOOP_DIR/hadoop jar $JAR_FILE novoproxy $HADOOP_CONF $HADOOP_OPTIONS \
		-in $FS_DIR/tier2_2 \
		-out $FS_DIR/tier3_1 \
		-novo $NOVOCRAFT_DIR \
		-d $NOVO_INDEX \
		-H -i $FRAG_LEN_AVG $FRAG_LEN_STDEV -r Ex 1100 -t 300 -o SAM \
		-fout bed \
		-D mapred.job.name="hydra.tier3.alignment" \
		-D mapred.reduce.tasks=$HADOOP_MAPRED_REDUCE_TASKS \
		-D io.sort.mb=100 \
		-D mapred.child.java.opts=-Xmx300m \
		-D mapred.task.timeout=$TIMEOUT_LONG
		
	checkStatus	
else
	out "Tier 3: Skipping alignment: $FS_DIR/tier3_1 already exists."
fi


## Pull down and merge the novoproxy output into a single BED file  

if [ ! -s tier3.bed ]
then
	out "Tier 3: Downloading BED file from HDFS."
	 
	$HADOOP_DIR/hadoop fs $HADOOP_CONF $HADOOP_OPTIONS -getmerge $FS_DIR/tier3_1 tier3.bed
	checkStatus
	
	out "Tier 3: Download complete."
else 
	out "Tier 3: tier3.bed already exists."
fi


## Find discordant pairs

if [ ! -s tier3.disc.bedpe ]
then
	out "Tier 3: Finding discordant pairs (z=$MAX_CONCORDANT_RANGE) (--> tier3.bed, tier3.disc.bedpe)"
  
	$SCRIPTS_DIR/pairDiscordants.py \
		-i tier3.bed \
		-m hydra \
		-z $MAX_CONCORDANT_RANGE > tier3.disc.bedpe

	checkStatus
else
	out "Tier 3: Skipping discordant pair find: tier3.disc.bedpe already exists." 
fi

if [ ! -s tier3.disc.bedpe ]
then
	doExit $E_INTERNAL "Tier 3: No discordant pairs found? (tier3.disc.bedpe has 0 length). Aborting."
fi


## Dedupe discordant pairs (dedupDiscordants.py)

if [ ! -s tier3.disc.deduped.bedpe ]
then
	out "Tier 3: De-duping discordant pairs (--> tier3.disc.deduped.bedpe)"
  
	$SCRIPTS_DIR/dedupDiscordants.py \
			-i tier3.disc.bedpe \
			-s 3 > tier3.disc.deduped.bedpe
  
	if [ ! -s tier3.disc.deduped.bedpe ]
	then
		doExit $E_EXTERNAL "Dedupe: FAILED. Aborting."
	fi
else
	out "Tier 3: Skipping de-duping discordant pairs: tier3.disc.deduped.bedpe already exists." 
fi


## We've reached the final step: check over the miscellaneous hydra 
## flags, and if they're defined then we append them to the command 
## parameters list when we call Hydra. 

if [ ! -z $HYDRA_MS ]
then
	hydraParams="$hydraParams -ms $HYDRA_MS"
fi

if [ ! -z $HYDRA_LNK ]
then
	hydraParams="$hydraParams -lnk $HYDRA_LNK"
fi

if [ ! -z $HYDRA_IS ]
then
	hydraParams="$hydraParams -is $HYDRA_IS"
fi

if [ ! -z $HYDRA_LI ]
then
	hydraParams="$hydraParams -li $HYDRA_LI"
fi

if [ ! -z $HYDRA_USE ]
then
	hydraParams="$hydraParams -use $HYDRA_USE"
fi

if [ ! -s sample.breaks ] 
then
	out "Tier 3: Calling hydra (--> sample.breaks)"
  
	$HYDRA_DIR/hydra -in tier3.disc.deduped.bedpe \
		-out sample.breaks \
		-mld $HYDRA_MLD \
		-mno $HYDRA_MNO $hydraParams >> $LOG_FILE 2>&1

	checkStatus
else
	out "Tier 3: Skipping Hydra call: sample.breaks already exists." 
fi

wait 
    
doExit $E_OK "$(date): Hydra pipeline completed (pwd=$PWD)"
