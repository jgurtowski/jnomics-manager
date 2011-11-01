#!/bin/bash

############################################################################
## 
##  Hydra pipeline script. Schatz Lab, Cold Spring Harbor Labs, 2011 
## 
##  Greatly extended by Matthew Titmus (mtitmus@cshl.edu) and Michael Schatz
##  (mschatz@cshl.edu) from a version originally written by Aaron Quinlan; it 
##  is available at http://code.google.com/p/hydra-sv/wiki/TypicalWorkflow
##
##  Revision: 800
##  Date: Thu Sep 15 01:11:59 EDT 2011
##
############################################################################


###[ Pipeline component directories ########################################
##
## Directories for dependent components may be specified here. 

# BEDTools; includes bamToBed utility. These are packaged with hydra, but 
# also available as a standalone utility (http://bedtools.googlecode.com) 
BEDTOOLS_DIR=

# BWA - Burrows-Wheeler Alignment Tool. Used for the initial alignment 
# performed in tier1.
BWA_DIR=

# The FASTX toolkit, which includes the fastq_quality_trimmer. Available at 
# http://hannonlab.cshl.edu/fastx_toolkit
FASTX_DIR=

# The Hadoop directory
HADOOP_DIR=

# The Hydra package, which provides the primary functions of this pipeline. 
# Available for download at http://code.google.com/p/hydra-sv
HYDRA_DIR=

# If blank, this will default to $HYDRA_DIR/../scripts
HYDRA_SCRIPTS_DIR=

# The directory that contains the Novoalign tool binary. Available from 
# Novocraft (http://www.novocraft.com)
#NOVOCRAFT_DIR=/bluearc/data/schatz/software/packages/novocraft-2.07.06
NOVOCRAFT_DIR=

# SAMTools directory (http://samtools.sourceforge.net).
SAMTOOLS_DIR=

# Contains the Python scripts used in tier 3. 
#SCRIPTS_DIR=/data/schatz/software/scripts   # Contains matchpairs.pl
SCRIPTS_DIR=


###[ Command parameter properties ]#########################################

# The location of the BAM file. If the $SAM_FILE doesn't exist, it is 
# generated from this.
BAM_FILE=

# The number of lines given to each split batch.
BATCH_SIZE=40000000

# The prefix of the BWA index. 
BWA_PREFIX=

# The FASTA-formatted sequence file. If a novoalign index doesn't exist, this
# will be used to generate one. 
FASTA=

# The location of the FASTQ read pair files.
FQ1_FILE=
FQ2_FILE=

# The pre-indexed Novoalign database file.
NOVO_INDEX=

# The number of threads to use for sequence alignment. If not specified, the 
# aligners (BWA and/or Novoalign) will use their defaults.
ALIGN_THREADS=
 
# Memory to allocate to samtools sort. If > RAM a segfault results.
SAMSORT_MAX_BYTES=500000000
  
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


############################################################################
##  Script Functions
############################################################################


############################################################################
## FUNCTION: calculateSizeStats()
## 
## If $FRAG_LEN_AVG and $FRAG_LEN_STDEV are undefined, this scans the first 
## ~10000 reads in $BAM_FILE and calculates the template length mean and
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

		if [ -z "$BAM_FILE" ] || [ ! -e "$BAM_FILE" ]
		then
			echo "calculateSizeStats: \$BAM_FILE undefined or does not exist (\"$BAM_FILE\")" >&2
			exit $E_EXTERNAL
		fi
		
		if [ -z "$SAMSTATS_DIR" ] || [ ! -e "$SAMSTATS_DIR" ]
		then
			echo "$(basename $0): Cannot find samStats.py in scripts directory or on the command path." >&2
			exit $E_EXTERNAL
		fi

		sizeStats=$($SAMTOOLS_DIR/samtools view $BAM_FILE | samStats.py -i stdin -n 10000 -r 0)
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
	pipestatus="${#PIPESTATUS[*]}\t${PIPESTATUS[@]}"
	pipecount=$(echo -e "$pipestatus" | cut -f 1)
	statuslist=$(echo -e "$pipestatus" | cut -f 2)
	lineNumber=

	if [ "$1" == "-L" ]
	then
		lineNumber=$2
	fi

	if [ ! -z "$lineNumber" ]
	then
		lineNumber="Line $lineNumber: "
	fi

	index=1
	for status in $statuslist
	do	
		if [[ $status -ne 0 ]]
		then
			if [ $pipecount -gt 1 ]
			then
				prefix="Piped command $index of $pipecount"
			else
				prefix="Command"
			fi

			if [[ $status -eq 126 ]]
			then
				msg="$prefix cannot execute (permissions problem?)"
			elif [[ $status -eq 127 ]]
			then
				msg="$prefix not found"
			elif [[ $status -gt 128 ]]
			then
				msg="$prefix got signal $(($status-128))"
	
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

			doExit $status "$lineNumber$msg (status=$status)"
		fi

		index=$[ $index + 1 ]
	done
}

alias checkStatus='checkStatus -L $LINENO'
shopt -s expand_aliases


############################################################################
## FUNCTION: killForks
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
##  

syntax() {
	echo "Usage: $(basename $0) (-n|-f) <index-file> -mld <bp> -mno <bp> [OPTION]" 
	echo "       $(basename $0) -L <savename>"
	echo
	echo "  -f <db.fasta>  REQUIRED. The FASTA-formatted sequence file."
	echo "  -n <file>      The pre-indexed Novoalign database file. Default: (fasta).ndx" 
	echo "  -p <prefix>    Prefix of the BWA index. Default=FASTA file name."
	echo 
	echo "Short-read input options (ANY ONE REQUIRED):"
	echo "  -fq <fq> <fq>  Fastq read pair files."
	echo "  -sam <file>    Path of an aligned SAM file. Default=tier1.sam"
	echo "  -bam <file>    Path of an aligned BAM file. Default=tier1.bam" 
	echo
	echo "Read processing and filtering options:"
	echo "  -b             Number of lines ber batch. A typical value for a"
	echo "                 genome-sized problem is about 40000000. Default=$BATCH_SIZE"
	echo "  -l <len>       Minimum fragment length. Sequences shorter than this (after " 
	echo "                 trimming) will be discarded. 0 = No min. Default=$FASTQ_MIN_SEQ_LEN."
	echo "  -m             Maximum memory (in kbytes) to allocate to samtools sort. "
	echo "                 Default: $[ $SAMSORT_MAX_BYTES / 1000 ] kb."
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
## FUNCTION: verifyNovoIndex()
##
## Determines whether the reference index and/or FASTA files are
## specified/exist. If the FASTA is specified but the index is not, this will
## look for the index as $FASTA.ndx and create it if it doesn't exist. 

GENERATED_KMERLEN=14
GENERATED_STEP=1

verifyNovoIndex() {
 	if [ -z $NOVO_INDEX ] && [ ! -z $FASTA ]
 	then
		NOVO_INDEX=$FASTA.ndx
 	fi
 	
	if [ ! -e $NOVO_INDEX ]
	then
		msg="(k=$GENERATED_KMERLEN s=$GENERATED_STEP)"
		
		out "Tier 2: Novoindexed reference sequence not found: building now ($msg). (You're welcome)"
		
		if [ -z $FASTA ] || [ ! -e $FASTA ]
		then
			msg="Reference sequence build failed: can't find FASTA file ($FASTA)." 
			out "Tier 2: $msg"
			doExit $E_BAD_PARAM $msg
		else
			$NOVOCRAFT_DIR/novoindex -k $GENERATED_KMERLEN -s $GENERATED_STEP -m $NOVO_INDEX $FASTA
			
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
## FUNCTION: verifyBamExists()
##
## Searches for the BAM/SAM files (guessing their names if not specified).
## If a SAM exists but a BAM does not, it will convert the file to a BAM.
## Finally, if a BAM can be found or created, this will create a soft 
## reference to the BAM from tier1.bam

verifyBamExists() {	
	# If no BAM is specified, we will look for a SAM file of the same name. 
	# If it exists we assume they're named identically.
	
	out "Searching for SAM/BAM files:"
	
	if [ -z "$BAM_FILE" ] && [ -z "$SAM_FILE" ]
	then
		BAM_FILE=tier1.bam
		SAM_FILE=tier1.sam
	elif [ -z "$BAM_FILE" ]
	then
		# We have a BAM, but no SAM. That's okay.
		BAM_FILE=$(echo "$SAM_FILE" | sed 's/.sam/.bam/')
	elif [ -z "$SAM_FILE" ]
	then
		# We have a BAM, but no SAM. That's okay.
		SAM_FILE=$(echo "$BAM_FILE" | sed 's/.bam/.sam/')
	fi

	# Check for the presence of the SAM and BAM files.
	
	if [ -e "$BAM_FILE" ]
	then
		out "   BAM: $BAM_FILE [\033[1;32mFOUND\033[0m]"

		if [ "$BAM_FILE" != "tier1.bam" ] 
		then
			ln -s $BAM_FILE tier1.bam 2> /dev/null
		fi
	else
		out "   BAM: $BAM_FILE [\033[1;31mNOT FOUND\033[0m]"
	fi 

	if [ -e "$SAM_FILE" ]
	then
		out "   SAM: $SAM_FILE [\033[1;32mFOUND\033[0m]"

		if [ "$SAM_FILE" != "tier1.sam" ] 
		then
			ln -s $SAM_FILE tier1.sam 2> /dev/null
		fi
	else
		out "   SAM: $SAM_FILE [\033[1;31mNOT FOUND\033[0m]"
	fi

	# Now that we've settled on file candidates, decide whether to convert or ignore

	if [ -e "$SAM_FILE" ] && [ ! -e "$BAM_FILE" ]
	then
		# The SAM exists but the BAM does not: convert.

		out "Tier 1: Found SAM but no BAM; converting SAM to sorted BAM..."

		$SAMTOOLS_DIR/samtools view -Su $SAM_FILE | \
			$SAMTOOLS_DIR/samtools sort -m $SAMSORT_MAX_BYTES - tier1.sorted
		checkStatus
	fi
}


############################################################################
## Start script body
############################################################################
	 
echo "Hydra pipeline script (Hydra-Hadoop). Revision 800 (Thu Sep 15 01:11:59 EDT 2011)"
echo "Matthew A. Titmus, Cold Spring Harbor Labs, 2011"
echo
	
# No args? Just output the command syntax and exit.
if [ ! -n "$1" ]
then
	syntax
	exit 0
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
		
		"-b" | "--batch-size" )
			(( index++ ))
			BATCH_SIZE=${!index}
		;;
		
		"-l" | "--fq-seqlen" )
			(( index++ ))
			FASTQ_MIN_SEQ_LEN=${!index}
		;; 

		"-m" | "--sort-bytes" )
			(( index++ ))
			SAMSORT_MAX_BYTES=$((${!index}*1000))  # Kbytes to bytes
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

if [ -z "$BEDTOOLS_DIR" ]
then
	BEDTOOLS_DIR=$(fullyResolveDir bamToBed)
fi
if [ ! -e "$BEDTOOLS_DIR" ]
then
	echo "Cannot find BEDTools directory: $BEDTOOLS_DIR" >&2
	echo "Please verify that it is correctly defined in the script parameters." >&2
	exit 1
fi

if [ -z "$BWA_DIR" ]
then
	BWA_DIR=$(fullyResolveDir bwa)
fi
if [ ! -e "$BWA_DIR" ]
then
	echo "Cannot find BWA directory: $BWA_DIR" >&2
	echo "Please verify that it is correctly defined in the script parameters." >&2
	exit 1
fi

if [ -z "$FASTX_DIR" ]
then
	FASTX_DIR=$(fullyResolveDir fastq_quality_trimmer)
fi
if [ ! -e "$FASTX_DIR" ]
then
	echo "Cannot find  FASTX toolkit directory: $FASTX_DIR" >&2
	echo "Please verify that it is correctly defined in the script parameters." >&2
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

if [ -z "$SAMTOOLS_DIR" ]
then
	SAMTOOLS_DIR=$(fullyResolveDir samtools)
fi
if [ ! -e "$SAMTOOLS_DIR" ]
then
	echo "Cannot find SAMTools directory: $SAMTOOLS_DIR" >&2
	echo "Please verify that it is correctly defined in the script parameters." >&2
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


############################################################################
# Check and/or determine maximum RAM allocated to SAMTools sort

MEM_TOTAL_BYTES=$(cat /proc/meminfo | grep 'MemTotal' | awk '{print $2}')
MEM_TOTAL_BYTES=$(($MEM_TOTAL_BYTES*1000)) # kB to bytes

if [ $MEM_TOTAL_BYTES -eq 0 ]
then
	# /proc/meminfo read failed for some reason. Do NOT let this value be 0!
	out "Failed to determine total RAM. Using default (500000kb)"
	SAMSORT_MAX_BYTES=500000000
elif [ -z $SAMSORT_MAX_BYTES ] || [ $SAMSORT_MAX_BYTES -eq 0 ]
then
	out "Sort memory max not specified or is zero; using total RAM" \
		"($(($MEM_TOTAL_BYTES/1000)) kB)"
	SAMSORT_MAX_BYTES=$MEM_TOTAL_BYTES 
else
	if (( $SAMSORT_MAX_BYTES > $MEM_TOTAL_BYTES ))
	then
		doExit $E_BAD_PARAM \
			"ERROR: Maximum sort bytes > total available RAM" \
			"($SAMSORT_MAX_BYTES vs $MEM_TOTAL_BYTES)"
	fi
fi

out "Hydra pipeline started: $(date)"


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

if [ -s tier1.sorted.bam ]
then
	out "Tier 1: tier1.sorted.bam already exists; advancing to Tier 2."
else
	## Does the indicated SAM file exist? If a BAM file already exists, this function 
	# creates a soft link to it from 'tier1.bam'
	verifyBamExists

	if [ -e "tier1.bam" ]
	then
		out "Tier 1: $BAM_FILE already exists; skipping tier 1 alignment."
	else		
		if [ -z "$FQ1_FILE" ] || [ -z "$FQ2_FILE" ]
		then
			doExit $E_BAD_PARAM "Tier 1: $BAM_FILE was not found and no fastq files were specified (-fq flag)"
		else
			out "Tier 1: $BAM_FILE not found; continuing with tier 1 alignment."
		fi
		  
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
	
		ALN_ARGS=
		if [ ! -z "$ALIGN_THREADS" ]
		then
			ALN_ARGS=" $ALN_ARGS -t $ALIGN_THREADS"
		fi
	  	
		out "Tier 1: Performing BWA alignment #1"
		echo "Tier 1: DOING: $BWA_DIR/bwa aln $ALN_ARGS $BWA_PREFIX $FQ1_FILE > $FQ1_FILE.sai" >> $LOG_FILE
		$BWA_DIR/bwa aln $ALN_ARGS $BWA_PREFIX $FQ1_FILE > $FQ1_FILE.sai 2>> $LOG_FILE
		checkStatus
	  	
		out "Tier 1: Performing BWA alignment #2"
		echo "Tier 1: DOING: $BWA_DIR/bwa aln $ALN_ARGS $BWA_PREFIX $FQ2_FILE > $FQ2_FILE.sai" >> $LOG_FILE
		$BWA_DIR/bwa aln $ALN_ARGS $BWA_PREFIX $FQ2_FILE > $FQ2_FILE.sai 2>> $LOG_FILE
		checkStatus
	  	
	  	out "Tier 1: Generating alignments of paired-end reads."
	  	echo "Tier 1: DOING: $BWA_DIR/bwa sampe $BWA_PREFIX $FQ1_FILE.sai $FQ2_FILE.sai $FQ1_FILE $FQ2_FILE" >> $LOG_FILE
	  	$BWA_DIR/bwa sampe $BWA_PREFIX $FQ1_FILE.sai $FQ2_FILE.sai $FQ1_FILE $FQ2_FILE | \
	  		samtools view -Su - > $BAM_FILE
		checkStatus
	  	
		if [ "$BAM_FILE" != "tier1.bam" ]
		then
			ln -s $BAM_FILE tier1.bam
			checkStatus
		fi
	fi

	calculateSizeStats

	out "Tier 1: Got basic read size statistics:"
	out "    Average length/stddev:  $FRAG_LEN_AVG / $FRAG_LEN_STDEV"
	out "    Median absolute dev:    $FRAG_LEN_MAD"
	out "    Max length difference:  $HYDRA_MLD"
	out "    Max non-overlap:        $HYDRA_MNO"
	out "    Max concordant range:   $MAX_CONCORDANT_RANGE"

	## Generate simple stats. This will be used later to determine the average
	## fragment lengths and standard deviations to feed into Hydra.
	##
	## (Should we move this to further in the pipeline?)

	if [ ! -s tier1.bam.stats ]
	then
		out "Tier 1: Generating tier 1 stats (tier1.bam.stats)"
		$SAMTOOLS_DIR/samtools flagstat tier1.bam > tier1.bam.stats
		checkStatus
	else
		out "Tier 1: Skipping stats generation: tier1.bam.stats already exists."
	fi

	## Generate additional read count statistics, separated by instrument and 
	## flow cell lane.
	(	
		IFILE=tier1.bam
		OFILE=tier1.bam.counts

		if [ -s $OFILE ]
		then
			out "Tier 1: Skipping $OFILE: File exists."
		else
			out "Tier 1: Spawning read tabulator process in background"
		
			$SAMTOOLS_DIR/samtools view $IFILE | \
				readcount.py -i stdin -o $OFILE >> $OFILE.log 2>&1
		fi
	) &	

	## Sort the alignments by readname, so we can correctly dump pairs.
	## Takes about 30 hours.

	if [ ! -s tier1.sorted.bam ]
	then
		if [ ! -z $SAMSORT_MAX_BYTES ]
		then
			SAMSORT_MAX_BYTES="-m $SAMSORT_MAX_BYTES"
		fi

		out "Tier 1: Sorting alignments by readname (tier1.sorted.bam)"
	
		$SAMTOOLS_DIR/samtools sort -n $SAMSORT_MAX_BYTES tier1.bam tier1.sorted
		checkStatus
	else
		out "Tier 1: Skipping alignment sort: tier1.sorted.bam already exists."
	fi
fi



############################################################################
## Tier 2 alignment. Here we grab the discordant alignments from the tier 1
## BAM files and create FASTQ files for the discordant pairs. We then align
## the tier 1 discordant pairs with a more sensitive aligner (Novoalign in 
## this case) using a word size of no more than 15bp (we typically use 
## 11-14 with Novoalign at this stage). This is necessary as many concordant
## pairs are missed in the first tier (a consequence of the tradeoff b/w 
## speed and senitivity), which lead to false positive SV calls. At this 
## point, you are merely trying to eliminate remaining concordant pairs. 
## Thus, we use the "-r Random" alignment mode for Novoalign. 

out "***********[ Tier 2 ]***********"

## Find discordant tier1 pairs

if [ ! -s tier1.disc.1.fq ] || [ ! -s tier1.disc.2.fq ]
then
	out "Tier 2: Finding discordant pairs "
	
	$SAMTOOLS_DIR/samtools view -uF 2 tier1.sorted.bam | \
		$HYDRA_DIR/bamToFastq -bam stdin \
			-fq1 tier1.disc.1.fq \
			-fq2 tier1.disc.2.fq
			
	checkStatus
else
	out "Tier 2: Skipping discordant pair identification: tier1.disc.?.fq exists."
fi

## Test for reasonable output; fail if lacking.

if [ ! -s tier1.disc.1.fq ]
then
	out "Tier 2: ERROR: No discordant pairs found (tier1.disc.1.fq has 0 length)"
	
	doExit $E_EXTERNAL \
	       "No discordant pairs found (tier1.disc.1.fq has 0 length)"
fi

## First aggressively trim the reads to clean up noisy bases
## plus it is too slow to use untrimmed reads: 51Mb in 7 hours :(
## Takes ~1.5 hours to trim

if [ ! -s tier1.disc.1.trim.fq ] || [ ! -s tier1.disc.2.trim.fq ]
then
	if [ ! -z $FASTQ_MIN_SEQ_LEN ]
	then
		FASTQ_MIN_SEQ_LEN="-l $FASTQ_MIN_SEQ_LEN"
	fi
	
	if [ ! -z $FASTQ_QUAL_THRESHOLD ] 
	then
		FASTQ_QUAL_THRESHOLD="-t $FASTQ_QUAL_THRESHOLD"
	fi
	
	# The -Q parameter is undocumented; per the release notes 
	# (http://hannonlab.cshl.edu/fastx_toolkit/): "-Q NN handles FASTQ ASCII 
	# quality with user specified offset (was hard-coded as 64 in previous 
	# versions)."

	out "Tier 2: Aggressively trimming reads (part 1/2)"
	
	$FASTX_DIR/fastq_quality_trimmer $FASTQ_QUAL_THRESHOLD -Q 33 \
			$FASTQ_MIN_SEQ_LEN < tier1.disc.1.fq > tier1.disc.1.trim.fq &
	
	out "Tier 2: Aggressively trimming reads (part 2/2)"
	
	$FASTX_DIR/fastq_quality_trimmer $FASTQ_QUAL_THRESHOLD -Q 33 \
			$FASTQ_MIN_SEQ_LEN < tier1.disc.2.fq > tier1.disc.2.trim.fq &
		
	wait
		
	if [ ! -s tier1.disc.1.trim.fq ]
	then
		doExit $E_EXTERNAL \
			   "Tier 2: fastq_quality_trimmer produced size 0 file. Aborting."
	fi
else
	out "Tier 2: Skipping aggressive read trim: tier1.disc.?.trim.fq exists."
fi

## Fix up broken pairs; takes about 40 minutes

if [ ! -s tier1.disc.1.trim.fq.matched ] || [ ! -s tier1.disc.2.trim.fq.matched ]
then
	out "Tier 2: Fixing up broken pairs"
	
	$SCRIPTS_DIR/matchpairs.pl tier1.disc.1.trim.fq tier1.disc.2.trim.fq >& matchpairs.log
	
	checkStatus
else
	out "Tier 2: Skipping broken pair fix-up: tier1.disc.?.trim.fq.matched exists."
fi

## Spawn another read tabulator process (2 of 5)

(	
	IFILE="tier1.*.matched"
	OFILE=tier1.matched.counts
	
	if [ -s $OFILE ]
	then
		out "Tier 2: Skipping $OFILE: File exists."
	else
		out "Tier 2: Spawning read tabulator process in background"
		
		readcount.py -i "$IFILE" -o $OFILE >> $OFILE.log 2>&1
	fi
) &	


## Split into batches of $BATCH_SIZE reads (default = 10^7)

if [ ! -s tier1.disc.1.trim.fq.matched.aa ]
then
	# Ensure that the batch size is a multiple of 4. 
	BATCH_SIZE=$[ $[ $BATCH_SIZE/4 ] * 4 ]
	
	out "Tier 2: Splitting match files into batches. (Batch size=$BATCH_SIZE)"
	
	(
		split -l $BATCH_SIZE tier1.disc.1.trim.fq.matched tier1.disc.1.trim.fq.matched.
		
		# If split exits with a non-0 code, remove the files (if any) and 
		# kill any other subprocesses.
		status=$?
		if [ ! $status -eq 0 ]
		then
			out "An unknown error has occured while the performing split action"
			rm tier1.disc.1.trim.fq.matched.?? >> /dev/null 2>&1
			killForks
		fi
	) &
	
	(
		split -l $BATCH_SIZE tier1.disc.2.trim.fq.matched tier1.disc.2.trim.fq.matched. 

		# If split exits with a non-0 code, remove the files (if any) and 
		# kill any other subprocesses.
		status=$?
		if [ ! $status -eq 0 ]
		then
			out "An unknown error has occured while the performing split action"
			rm tier1.disc.2.trim.fq.matched.?? >> /dev/null 2>&1
			killForks
		fi
	) &
	
	wait
	
	if [ -s tier1.disc.1.trim.fq.matched.aa ] && [ -s tier1.disc.2.trim.fq.matched.aa ]
	then
		out "Split complete. $(ls -l tier1.disc.1.trim.fq.matched.?? | wc -l)" \
			"* 2 files created."
	else
		rm tier1.disc.?.trim.fq.matched.?? >> /dev/null 2>&1
		doExit $E_EXTERNAL "An error occured with the file split process"
		exit
	fi
else
	out "Tier 2: Skipping batch file creation: tier1.disc.?.trim.fq.matched.aa already exists."
fi


out "Tier 2: Checking alignments against tier 1"

## If the number of threads dedicated to Novoalign isn't specified, we allow it
## to use the default value (the number of available cores).

if [ -z "$NOVO_THREADS" ]
then
	out "Tier 2: Using default threads argument for Novo-alignment."
	NOVO_THREADS=""
else
	out "Tier 2: Using $NOVO_THREADS threads for alignment."
fi

for a in $(/bin/ls tier1.disc.1.trim.fq.matched.??);
do
	c=$(echo $a | cut -f7 -d'.')
	b=$(echo $a | sed 's/tier1.disc.1/tier1.disc.2/')
	    
	if [ ! -s tier2.$c.bam.stats ]
	then
		verifyNovoIndex
		    
		out "   $c"

		echo "Tier 2: DOING: $NOVOCRAFT_DIR/novoalign $NOVO_THREADS -d $NOVO_INDEX -H -f $a $b -i $FRAG_LEN_AVG $FRAG_LEN_STDEV -r Random -o SAM" >> $LOG_FILE
		($NOVOCRAFT_DIR/novoalign $NOVO_THREADS -d $NOVO_INDEX -H \
					-f $a $b \
					-i $FRAG_LEN_AVG $FRAG_LEN_STDEV -r Random -o SAM | \
			$SAMTOOLS_DIR/samtools view -Sb - > tier2.$c.bam) >& tier2.$c.log
		
		checkStatus

		$SAMTOOLS_DIR/samtools flagstat tier2.$c.bam > tier2.$c.bam.stats
	fi
done


## Spawn another read tabulator process (3 of 5)

(	
	IFILE=tier2.??.bam
	OFILE=tier2.bam.counts
	
	# For each batch file, pipe BAM-formatted text to readcount.py, which
	# reads the existing data from $OFILE, modifies it with the input from
	# stdin, and then rewrites the updated values to the same file.
	
	if [ -s $OFILE ]
	then
		out "Tier 2: Skipping $OFILE: File exists."
	else
		out "Tier 2: Spawning read tabulator process in background"
		
		for batch in $(/bin/ls $IFILE)
		do
			$SAMTOOLS_DIR/samtools view $batch | \
				readcount.py -i stdin -o $OFILE -a $OFILE \
				 >> $OFILE.log 2>&1
		done
	fi
) &	


## Find discordant pairs in each tier2 batch

out "Tier 2: Finding discordant pairs:"
for batch in $(/bin/ls tier2.??.bam)
do
	if [ ! -s $batch.1.fq ]
	then
		out "  $batch"
			
		$SAMTOOLS_DIR/samtools view -uF 2 $batch | \
			$HYDRA_DIR/bamToFastq -bam stdin \
				-fq1 $batch.1.fq \
				-fq2 $batch.2.fq
		
		checkStatus
	fi
done


## Match resulting pairs

out "Tier 2: Matching pairs:"
for pair in $(/bin/ls tier2.??.bam)
do
	if [ ! -s $pair.1.fq.matched ]
	then
		out "  $pair"
		$SCRIPTS_DIR/matchpairs.pl $pair.1.fq $pair.2.fq  >& matchpairs.tier2.log
	fi
done


## Spawn another read tabulator process (4 of 5)
(	
	IFILE=tier2.*.matched
	OFILE=tier2.matched.counts

	if [ -s $OFILE ]
	then
		out "Tier 2: Skipping $OFILE: File exists."
	else
		out "Tier 2: Spawning read tabulator process in background"
		
		readcount.py -i "$IFILE" -o $OFILE >> $OFILE.log 2>&1
	fi
) &	


############################################################################
## Tier 3

## Align to tier3 (in batches)

out "***********[ Tier 3 ]***********"

if [ ! -s tier3.bam.stats ]
then
	out "Tier 3: Batch aligning Tier 2 matches"
    
	for a in $(/bin/ls tier2.??.bam.1.fq.matched);
	do
		c=$(echo $a | cut -f2 -d'.')
		b=$(echo $a | sed 's/bam.1.fq.matched/bam.2.fq.matched/')

		if [ ! -s tier3.$c.bam.stats ]
		then
			out "  $c  "
			
			verifyNovoIndex

			($NOVOCRAFT_DIR/novoalign $NOVO_THREADS -d $NOVO_INDEX -H \
					-f $a $b \
					-i $FRAG_LEN_AVG $FRAG_LEN_STDEV -r Ex 1100 -t 300 -o SAM | \
				$SAMTOOLS_DIR/samtools view -Sb - > tier3.$c.bam) >& tier3.$c.log
	
			checkStatus "ERROR: Check tier3.$c.log (Error code=$?)"
			
			$SAMTOOLS_DIR/samtools flagstat tier3.$c.bam > tier3.$c.bam.stats
	
			checkStatus
		fi
	done
else
	out "Tier 3: Skipping batch alignment of tier 2 matches: tier3.bam.stats exists."
fi


## Merge all the batches into a single BAM file. Rate ~200mb/minute (roughly 4
## hours for a human genome)

if [ ! -s tier3.bam ]
then
	out  "Tier 3: Merging batch files (--> tier3.bam)"
	count=0
  
	args=$()
	for batch in $(/bin/ls tier3.??.bam)
	do
		 out "  $batch"
		(( count+=1 ))
		
		args="$args$batch "
	done

	if [ "$count" == 1 ]   # A single file won't merge, so copy it.
	then
		out  "  WARNING: Only one tier3.??.bam file found. Creating" \
				"soft link 'tier3.bam' to 'tier3.aa.bam'"
		ln -fs tier3.aa.bam tier3.bam
	else
		$SAMTOOLS_DIR/samtools merge tier3.bam $args
		checkStatus
	fi
  
	if [ ! -s tier3.bam ] 
	then
		doExit $E_EXTERNAL "Tier 3 merge failed. Aborting."
	else
		out "Merge complete."
	fi
else 
	out "Tier 3: Match file merge: tier3.bam already exists."
fi


## Spawn a read tabulator process. (5 of 5)

(	
	IFILE=tier3.bam
	OFILE=tier3.bam.counts

	if [ -s $OFILE ]
	then
		out "Tier 3: Skipping $OFILE: File exists."
	else
		out "Tier 3: Spawning read tabulator process in background"
		
		$SAMTOOLS_DIR/samtools view $IFILE | \
			readcount.py -i stdin -o $OFILE >> $OFILE.log 2>&1
	fi
) &	


## Find discordant pairs

if [ ! -s tier3.disc.bedpe ]
then
	out "Tier 3: Finding discordant pairs (z=$MAX_CONCORDANT_RANGE) (--> tier3.bed, tier3.disc.bedpe)"

	if [ ! -s tier3.bed ]
	then
		$BEDTOOLS_DIR/bamToBed -i tier3.bam -tag NM > tier3.bed
		checkStatus
	fi
  
	$HYDRA_SCRIPTS_DIR/pairDiscordants.py \
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
  
	$HYDRA_SCRIPTS_DIR/dedupDiscordants.py \
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

if [ -z $NO_CLEANUP ]
then
	out "Doing post-run cleanup..."
	rm -v tier1.sorted.bam >> $LOG_FILE 2>&1
	rm -v tier1.disc.* >> $LOG_FILE 2>&1
	rm -v tier2.*.?.* >> $LOG_FILE 2>&1
fi
    
doExit $E_OK "$(date): Hydra pipeline completed (pwd=$PWD)"
