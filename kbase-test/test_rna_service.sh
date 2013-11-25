#!/bin/bash

set -e

export PATH=/kb/deployment/bin:$PATH

TEST_DIR="test_rna_${$}"

echo "Test dir: ${TEST_DIR}"

function jmessage {
    echo 
    echo
    echo "#################"
    echo "#################"
    echo " $1"
    echo "#################"
    echo "#################"
    echo
    echo
    echo
    echo
}

function cleanup {
    rm -f t1.1.fq.gz t1.2.fq.gz t2.1.fq.gz t2.2.fq.gz
}


#remove local files
jmessage "Removing stale files"
cleanup

#run new stuff
jmessage "Downloading test data"
jkbase fs -get /share/example/t1.1.fq.gz
jkbase fs -get /share/example/t1.2.fq.gz
jkbase fs -get /share/example/t2.1.fq.gz
jkbase fs -get /share/example/t2.2.fq.gz

jmessage "Uploading test data to cluster"
jkbase fs -put t1.1.fq.gz ${TEST_DIR}/t1.1.fq.gz
jkbase fs -put t1.2.fq.gz ${TEST_DIR}/t1.2.fq.gz
jkbase fs -put t2.1.fq.gz ${TEST_DIR}/t2.1.fq.gz
jkbase fs -put t2.2.fq.gz ${TEST_DIR}/t2.2.fq.gz


jmessage "Aligning reads"
echo "jkbase compute tophat -in=${TEST_DIR}/t1.1.fq.gz,${TEST_DIR}/t1.2.fq.gz -ref=ecoli -out=${TEST_DIR}/t1_tophat"
echo "jkbase compute tophat -in=${TEST_DIR}/t2.1.fq.gz,${TEST_DIR}/t2.2.fq.gz -ref=ecoli -out=${TEST_DIR}/t2_tophat"
align_jobid1=`jkbase compute tophat -in=${TEST_DIR}/t1.1.fq.gz,${TEST_DIR}/t1.2.fq.gz -ref=ecoli -out=${TEST_DIR}/t1_tophat | grep Submitted | cut -d ':' -f 2 | awk '{print $1}'`
align_jobid2=`jkbase compute tophat -in=${TEST_DIR}/t2.1.fq.gz,${TEST_DIR}/t2.2.fq.gz -ref=ecoli -out=${TEST_DIR}/t2_tophat | grep Submitted | cut -d ':' -f 2 | awk '{print $1}'`

echo "Launched Job: $align_jobid1"
echo "Launched Job: $align_jobid2"

while [ ! `jkbase compute grid_job_status -job=${align_jobid1} | grep "^Job" | cut -d '-' -f 2 | awk '{print $1}'` == "DONE" ]
do
    sleep 10
    jkbase compute grid_job_status -job=${align_jobid1}
done

jkbase compute grid_job_status -job=${align_jobid2}

while [ ! `jkbase compute grid_job_status -job=${align_jobid2} | grep "^Job" | cut -d '-' -f 2 | awk '{print $1}'` == "DONE" ]
do
    sleep 10
    jkbase compute grid_job_status -job=${align_jobid2}
done

jmessage "Running Cufflinks"

echo "jkbase compute cufflinks -in=${TEST_DIR}/t1_tophat/accepted_hits.bam -out=${TEST_DIR}/t1_cufflinks"
cuff_job1=`jkbase compute cufflinks -in=${TEST_DIR}/t1_tophat/accepted_hits.bam -out=${TEST_DIR}/t1_cufflinks | grep Submitted | cut -d ':' -f 2 | awk '{print $1}'`
echo "jkbase compute cufflinks -in=${TEST_DIR}/t2_tophat/accepted_hits.bam -out=${TEST_DIR}/t2_cufflinks"
cuff_job2=`jkbase compute cufflinks -in=${TEST_DIR}/t2_tophat/accepted_hits.bam -out=${TEST_DIR}/t2_cufflinks | grep Submitted | cut -d ':' -f 2 | awk '{print $1}'`

while [ ! `jkbase compute grid_job_status -job=${cuff_job1} | grep "^Job" | cut -d '-' -f 2 | awk '{print $1}'` == "DONE" ]
do
    sleep 10
    jkbase compute grid_job_status -job=${cuff_job1}
done

jkbase compute grid_job_status -job=${cuff_job2}
while [ ! `jkbase compute grid_job_status -job=${cuff_job2} | grep "^Job" | cut -d '-' -f 2 | awk '{print $1}'` == "DONE" ]
do
    sleep 10
    jkbase compute grid_job_status -job=${cuff_job2}
done


jmessage "Calling Cuffmerge"
echo "jkbase compute cuffmerge -in=${TEST_DIR}/t1_cufflinks/transcripts.gtf,${TEST_DIR}/t2_cufflinks/transcripts.gtf -ref=ecoli -out=${TEST_DIR}/cuffmerge_out"
cuffmerge_job=`jkbase compute cuffmerge -in=${TEST_DIR}/t1_cufflinks/transcripts.gtf,${TEST_DIR}/t2_cufflinks/transcripts.gtf -ref=ecoli -out=${TEST_DIR}/cuffmerge_out | grep Submitted | cut -d ':' -f 2 | awk '{print $1}'`

echo "Submitted Job: ${cuffmerge_job}" 
jkbase compute grid_job_status -job=${cuffmerge_job}
while [ ! `jkbase compute grid_job_status -job=${cuffmerge_job} | grep "^Job" | cut -d '-' -f 2 | awk '{print $1}'` == "DONE" ]
do
    sleep 10
    jkbase compute grid_job_status -job=${cuffmerge_job}
done


jmessage "Call Cuffdiff"

echo "jkbase compute cuffdiff -in=${TEST_DIR}/t1_tophat/accepted_hits.bam,${TEST_DIR}/t2_tophat/accepted_hits.bam -out=${TEST_DIR}/cuffdiff_out -ref=ecoli -condn_labels=T1,T2 -merged_gtf=${TEST_DIR}/cuffmerge_out/merged.gtf"

cuffdiff_job=`jkbase compute cuffdiff -in=${TEST_DIR}/t1_tophat/accepted_hits.bam,${TEST_DIR}/t2_tophat/accepted_hits.bam -out=${TEST_DIR}/cuffdiff_out -ref=ecoli -condn_labels=T1,T2 -merged_gtf=${TEST_DIR}/cuffmerge_out/merged.gtf | grep Submitted | cut -d ':' -f 2 | awk '{print $1}'`

while [ ! `jkbase compute grid_job_status -job=${cuffdiff_job} | grep "^Job" | cut -d '-' -f 2 | awk '{print $1}'` == "DONE" ]
do
    sleep 10
    jkbase compute grid_job_status -job=${cuffdiff_job}
done

jmessage "Call cuffcompare"

echo "jkbase compute cuffcompare -in=${TEST_DIR}/t1_cufflinks/transcripts.gtf,${TEST_DIR}/t2_cufflinks/transcripts.gtf -out=${TEST_DIR}/cuffcompare_out -ref_gtf=${TEST_DIR}/cuffmerge_out/merged.gtf"

cuffcompare_job=`jkbase compute cuffcompare -in=${TEST_DIR}/t1_cufflinks/transcripts.gtf,${TEST_DIR}/t2_cufflinks/transcripts.gtf -out=${TEST_DIR}/cuffcompare_out -ref_gtf=${TEST_DIR}/cuffmerge_out/merged.gtf | grep Submitted | cut -d ':' -f 2 | awk '{print $1}'`


while [ ! `jkbase compute grid_job_status -job=${cuffcompare_job} | grep "^Job" | cut -d '-' -f 2 | awk '{print $1}'` == "DONE" ]
do
    sleep 10
    jkbase compute grid_job_status -job=${cuffcompare_job}
done


jmessage "Tests Complete: PASSED, Have a nice day :)"

cleanup

