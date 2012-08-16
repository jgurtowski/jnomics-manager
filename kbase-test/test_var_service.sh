#!/bin/bash

set -e

export PATH=/kb/deployment/services/jnomics/bin:$PATH

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
#jkbase fs -rmr yeastrename_test.pe
#jkbase fs -rmr yeastrename_test_bwa
#jkbase fs -rmr yeastrename_test_snp
#jkbase fs -rm yeastrename_test.vcf

#run new stuff
jmessage "Downloading test data"
jkbase fs -get /share/yeastrename.1.fq
jkbase fs -get /share/yeastrename.2.fq

jmessage "Uploading test data to cluster"
jkbase fs -put_pe yeastrename.1.fq yeastrename.2.fq ${TEST_DIR}/yeastrename_test

jmessage "Aligning reads"
#run alignment
align_jobid=`jkbase compute bwa -in ${TEST_DIR}/yeastrename_test.pe -out ${TEST_DIR}/yeastrename_test_bwa -organism yeastrename | cut -d ':' -f 2`
echo "alignment job: ${align_jobid}"

while [ `jkbase compute status -job $align_jobid | grep Complete | awk '{print $2}'` == "false" ]
do
    sleep 10
    jkbase compute status -job ${align_jobid}
done

jmessage "Calling snps"
#run snp
snp_jobid=`jkbase compute snp -in ${TEST_DIR}/yeastrename_test_bwa -out ${TEST_DIR}/yeastrename_test_snp -organism yeastrename | cut -d ':' -f 2`
echo "snp job: ${snp_jobid}"

while [ `jkbase compute status -job $snp_jobid | grep Complete | awk '{print $2}'` == "false" ]
do
    sleep 10
    jkbase compute status -job $snp_jobid
done

jmessage "Merging vcf files"
#merge vcf
jkbase compute vcf_merge -alignments ${TEST_DIR}/yeastrename_test_bwa -in ${TEST_DIR}/yeastrename_test_snp -out ${TEST_DIR}/yeastrename_test.vcf

jmessage "Downloading complete vcf"
#download vcf
jkbase fs -get ${TEST_DIR}/yeastrename_test.vcf

rm -f yeastrename.1.fq yeastrename.2.fq 
