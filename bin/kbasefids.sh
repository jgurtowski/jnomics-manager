#!/bin/bash
SCRIPT_PATH=`dirname "$0"`
SORT_PATH=`which sort`
AWK_PATH=`which awk`
export PATH=$SGE_O_HOME:$TMPDIR:$PATH
echo $TMPDIR
    $SORT_PATH -k1,1 -V -k4,4 $2 > ./transcripts.gtf_sorted
    ./bedtools intersect -a $1 -b ./transcripts.gtf_sorted -wa -wb -f 0.90 > ./mapped_transcripts.gtf
    $AWK_PATH -F"\t" '{print $5"\t"$9"\t"$10"\t"$12"\t"$14}' ./mapped_transcripts.gtf > ./pre_kbase_transcripts.gtf
    /bin/sort -t'"' -k4,4 -V -u ./pre_kbase_transcripts.gtf > ./kbase_transcripts.gtf
    #/bin/ls -l $1/kbase_transcripts_pop2_final.gtf
echo "Process Complete"
