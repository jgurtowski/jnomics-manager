package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.io.ThreadedStreamConnector;
import edu.cshl.schatz.jnomics.ob.SAMHeaderSequence;
import edu.cshl.schatz.jnomics.ob.SAMRecordWritable;
import edu.cshl.schatz.jnomics.util.ProcessUtil;
import net.sf.picard.sam.AddOrReplaceReadGroups;
import net.sf.picard.sam.MarkDuplicates;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;

import javax.print.DocFlavor;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * User: james
 */
public class GATKRealignReduce extends GATKBaseReduce<SamtoolsMap.SamtoolsKey,SAMRecordWritable,SAMRecordWritable,NullWritable> {


    private final SAMRecordWritable recordWritable = new SAMRecordWritable();

    final Log log = LogFactory.getLog(GATKRealignReduce.class);

    @Override
    public Class getOutputKeyClass() {
        return SAMRecordWritable.class;
    }

    @Override
    public Class getOutputValueClass() {
        return NullWritable.class;
    }

    @Override
    public Class<? extends Partitioner> getPartitionerClass() {
        return SamtoolsReduce.SamtoolsPartitioner.class;
    }

    @Override
    public Class<? extends WritableComparator> getGrouperClass() {
        return SamtoolsReduce.SamtoolsGrouper.class;
    }

    @Override
    protected void reduce(SamtoolsMap.SamtoolsKey key, Iterable<SAMRecordWritable> values, final Context context)
            throws IOException, InterruptedException {

        final File tmpSam = new File(context.getTaskAttemptID()+".tmp.sam");
        long count =0;
        BufferedWriter writer = new BufferedWriter(new FileWriter(tmpSam));
        for(SAMRecordWritable record: values){
            if(0==count){
                writer.write(record.getTextHeader()+"\n");
            }
            writer.write(record + "\n");
            if(0 == ++count % 1000){
                context.progress();
            }
        }
        writer.close();
        

        /**Add ReadGroups**/
        System.out.println("Adding Read Groups");
        final File tmpBamrdGroups = new File(context.getTaskAttemptID()+".tmp.rdgrp.bam");
        new AddOrReplaceReadGroups(){
            {
                INPUT=tmpSam;
                OUTPUT=tmpBamrdGroups;
                RGID="1";
                RGLB="jnomics";
                RGPL="illumina";
                RGPU="abc";
                RGSM="jnomics-sample";
                CREATE_INDEX=true;
                VALIDATION_STRINGENCY=SAMFileReader.ValidationStringency.LENIENT;
                doWork();
            }
        };

        tmpSam.delete();

        /**Remove Duplicates**/
        final File tmpBamRmDup = new File(context.getTaskAttemptID()+".tmp.rdgrp.rmdup.bam");
        final File metricsFile = new File(context.getTaskAttemptID()+".metrics");
        //*String samtools_rmdup_cmd = String.format("%s rmdup %s %s", samtools_binary, tmpBam, tmpBamRmDup);
        /*ProcessUtil.exceptionOnError(ProcessUtil.execAndReconnect(samtools_rmdup_cmd));*/
        System.out.println("Removing Duplicates");
        new MarkDuplicates(){
            {
                INPUT=new ArrayList<File>(){
                    {
                        add(tmpBamrdGroups);
                    }
                };
                OUTPUT=tmpBamRmDup;
                ASSUME_SORTED=true;
                REMOVE_DUPLICATES=true;
                METRICS_FILE=metricsFile;
                CREATE_INDEX=true;
                VALIDATION_STRINGENCY= SAMFileReader.ValidationStringency.LENIENT;
                final File tmp_dir = new File("rmdups_tmp");
                tmp_dir.mkdir();
                TMP_DIR=new ArrayList<File>(){
                    {
                        add(tmp_dir);
                    }
                };
                tmp_dir.delete();
                doWork();
            }
        };
        tmpBamrdGroups.delete();
        metricsFile.delete();

        /**Index bam**/
        String samtools_idx_cmd = String.format("%s index %s", samtools_binary, tmpBamRmDup);
        ProcessUtil.exceptionOnError(ProcessUtil.execAndReconnect(samtools_idx_cmd));

        /**GATK Find Targets**/
        File intervals = new File(context.getTaskAttemptID()+".intervals");
        String find_targets_cmd = String.format("java -Xmx6g -jar %s -T RealignerTargetCreator -R %s -o %s -I %s",
                gatk_binary, reference_fa, intervals, tmpBamRmDup);
        ProcessUtil.exceptionOnError(ProcessUtil.execAndReconnect(find_targets_cmd));

        /**GATK Indel Realigner**/
        File realign_bam = new File(context.getTaskAttemptID()+".tmp.rmdup.realign.bam");
        String indel_realign_cmd = String.format("java -Xmx6g -jar %s -T IndelRealigner -R %s -targetIntervals %s -I %s -o %s",
                gatk_binary,reference_fa,intervals,tmpBamRmDup,realign_bam);
        ProcessUtil.exceptionOnError(ProcessUtil.execAndReconnect(indel_realign_cmd));

        tmpBamRmDup.delete();
        intervals.delete();
        
        SAMFileReader reader = new SAMFileReader(realign_bam,true);
        reader.setValidationStringency(SAMFileReader.ValidationStringency.LENIENT);
        for(SAMRecord record: reader){
            recordWritable.set(record);
            context.write(recordWritable,NullWritable.get());
        }

        realign_bam.delete();
        System.out.println();
        System.out.println("<----------------------------------------********--------------------------------------->");
        System.out.println();
    }
}
