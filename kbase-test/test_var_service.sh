#!/bin/bash

set -e

export PATH=/kb/deployment/bin:$PATH

TEST_DIR="test_${$}"

function jmessage {
    echo
    echo
    echo
    echo
    echo "##########################"
    echo "#"
    echo "# $1"
    echo "#"
    echo "##########################"
    echo
    echo
    echo
    echo
}

#remove local files
jmessage "Removing stale files"
rm -f yeastrename.1.fq yeastrename.2.fq 
rm -f yeastrename_test.vcf

#run new stuff
jmessage "Downloading test data"
jkbase fs -get /share/example/yeast_sim.1.fq.gz
jkbase fs -get /share/example/yeast_sim.2.fq.gz

jmessage "Uploading test data to cluster"
jkbase fs -put_pe yeast_sim.1.fq.gz yeast_sim.2.fq.gz ${TEST_DIR}/yeast_sim

jmessage "Aligning reads"
#run alignment
align_jobid=`jkbase compute bwa -in ${TEST_DIR}/yeast_sim.pe -out ${TEST_DIR}/yeast_sim_bwa -organism yeast | cut -d ':' -f 2`
echo "alignment job: ${align_jobid}"

while [ `jkbase compute status -job $align_jobid | grep Complete | awk '{print $2}'` == "false" ]
do
    sleep 10
    jkbase compute status -job ${align_jobid}
done

jmessage "Calling snps"
#run snp
snp_jobid=`jkbase compute snp -in ${TEST_DIR}/yeast_sim_bwa -out ${TEST_DIR}/yeast_sim_snp -organism yeast | cut -d ':' -f 2`
echo "snp job: ${snp_jobid}"

while [ `jkbase compute status -job $snp_jobid | grep Complete | awk '{print $2}'` == "false" ]
do
    sleep 10
    jkbase compute status -job $snp_jobid
done

jmessage "Merging vcf files"
#merge vcf
jkbase compute vcf_merge -alignments ${TEST_DIR}/yeast_sim_bwa -in ${TEST_DIR}/yeast_sim_snp -out ${TEST_DIR}/yeast_sim.vcf

jmessage "Downloading complete vcf"
#download vcf
jkbase fs -get ${TEST_DIR}/yeast_sim.vcf

jmessage "Tests Complete: PASSED, Have a nice day :)"

rm -f yeast_sim.1.fq.gz yeast_sim.2.fq.gz 

