#!/bin/bash

set -e

export PATH=/kb/deployment/bin:$PATH

TEST_DIR="test_${$}"

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

#remove local files
jmessage "Removing stale files"
rm -f yeast_sim.1.fq.gz yeast_sim.2.fq.gz yeast_sim.1.fq.gz.md5 yeast_sim.2.fq.gz.md5
rm -f yeast_sim.vcf yeast_sim_canonical.vcf

#run new stuff
jmessage "Downloading test data"
jkbase fs -get /share/example/yeast_sim.1.fq.gz
jkbase fs -get /share/example/yeast_sim.2.fq.gz

jkbase fs -get /share/example/yeast_sim.1.fq.gz.md5
jkbase fs -get /share/example/yeast_sim.2.fq.gz.md5

md5sum -c yeast_sim.1.fq.gz.md5
md5sum -c yeast_sim.2.fq.gz.md5


jmessage "Uploading test data to cluster"
jkbase fs -put_pe yeast_sim.1.fq.gz yeast_sim.2.fq.gz ${TEST_DIR}/yeast_sim

jmessage "Aligning reads"
#run alignment
align_jobid=`jkbase compute bwa -in=${TEST_DIR}/yeast_sim.pe -out=${TEST_DIR}/yeast_sim_bwa -org=yeast | cut -d ':' -f 2 | sed "s/^[ \t]*//"`
echo "alignment job:${align_jobid}"

while [ `jkbase compute status -job=${align_jobid} | grep Complete | awk '{print $2}'` == "false" ]
do
    sleep 10
    jkbase compute status -job=${align_jobid}
done

jmessage "Calling snps"
#run snp
snp_jobid=`jkbase compute samtools_snp -in=${TEST_DIR}/yeast_sim_bwa -out=${TEST_DIR}/yeast_sim_snp -org=yeast | cut -d ':' -f 2 | sed "s/^[ \t]*//"`
echo "snp job: ${snp_jobid}"

while [ `jkbase compute status -job=$snp_jobid | grep Complete | awk '{print $2}'` == "false" ]
do
    sleep 10
    jkbase compute status -job=$snp_jobid
done

jmessage "Merging vcf files"

#merge vcf
jkbase compute vcf_merge -aln=${TEST_DIR}/yeast_sim_bwa -in=${TEST_DIR}/yeast_sim_snp -out=${TEST_DIR}/yeast_sim.vcf

jmessage "Downloading complete vcf"

#download vcf
jkbase fs -get ${TEST_DIR}/yeast_sim.vcf

jkbase fs -get /share/example/yeast_sim.vcf yeast_sim_canonical.vcf

diff <(grep -v "#" yeast_sim.vcf ) <( grep -v "#" yeast_sim_canonical.vcf)

if [ $? != 0 ]
then
    jmessage "Error, VCF file does not match"
    exit 1
fi 

jmessage "Tests Complete: PASSED, Have a nice day :)"

rm -f yeast_sim.1.fq.gz yeast_sim.2.fq.gz yeast_sim.1.fq.gz.md5 yeast_sim.2.fq.gz.md5 

