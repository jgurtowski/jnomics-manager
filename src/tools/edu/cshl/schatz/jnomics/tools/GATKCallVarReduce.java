package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.io.ThreadedStreamConnector;
import edu.cshl.schatz.jnomics.ob.SAMRecordWritable;
import edu.cshl.schatz.jnomics.util.ProcessUtil;
import net.sf.samtools.SAMSequenceRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 * User: james
 */

public class GATKCallVarReduce extends GATKBaseReduce<SamtoolsMap.SamtoolsKey,SAMRecordWritable,NullWritable,NullWritable>{

    @Override
    public Class getOutputKeyClass() {
        return NullWritable.class;
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
    protected void reduce(SamtoolsMap.SamtoolsKey key, Iterable<SAMRecordWritable> values, Context context)
            throws IOException, InterruptedException {

        File tmpBam = new File(context.getTaskAttemptID()+".tmp.bam");
        /**Write bam to temp file**/
        String samtools_convert_cmd = String.format("%s view -Sb -o %s -", samtools_binary, tmpBam);
        System.out.println(samtools_convert_cmd);
        Process samtools_convert = Runtime.getRuntime().exec(samtools_convert_cmd);

        Thread errConn = new Thread(new ThreadedStreamConnector(samtools_convert.getErrorStream(),System.err));
        Thread outConn = new Thread(new ThreadedStreamConnector(samtools_convert.getInputStream(),System.out));
        outConn.start();errConn.start();

        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(samtools_convert.getOutputStream()));
        int ref_len=-1;
        long count = 0;
        for(SAMRecordWritable record: values){
            if(0 == count){
                writer.write(record.getTextHeader()+"\n");
                SAMSequenceRecord headerSequence = record.getSAMRecord().getHeader().getSequence(key.getRef().toString());
                if(null != headerSequence)
                    ref_len = headerSequence.getSequenceLength();
                if(ref_len < 0)
                    throw new IOException("Could not get Reference Length from header");
            }
            writer.write(record.toString()+"\n");
            if(0 == ++count % 1000 ){
                context.progress();
                context.write(NullWritable.get(),NullWritable.get());
            }
        }

        long startRange = key.getPosition().get();
        long endRange = key.getPosition().get() + binsize;
        endRange = endRange > ref_len ? ref_len : endRange;
        System.out.println("Working on Region:" + key.getRef() + ":" + startRange + "-" + endRange);

        writer.close();
        samtools_convert.waitFor();
        outConn.join();errConn.join();

        /**Index Bam**/
        String samtools_idx_cmd = String.format("%s index %s", samtools_binary, tmpBam);
        ProcessUtil.exceptionOnError(ProcessUtil.execAndReconnect(samtools_idx_cmd));

        /**Call Variations**/
        File recal_vcf = new File(key.getRef()+"-"+key.getBin()+".vcf");
        recal_vcf.createNewFile();

        String variation_cmd = String.format("java -Xmx3g -jar %s -T UnifiedGenotyper -L %s:%d-%d -R %s -I %s -o %s -stand_call_conf 50 -stand_emit_conf 10.0 -minIndelCnt 5 -indelHeterozygosity 0.0001 -nt 5",
                gatk_binary,key.getRef(),startRange,endRange,reference_fa,tmpBam,recal_vcf);
        ProcessUtil.exceptionOnError(ProcessUtil.execAndReconnect(variation_cmd));

        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        fs.copyFromLocalFile(new Path(recal_vcf.toString()), new Path(conf.get("mapred.output.dir")+"/"+recal_vcf));
        tmpBam.delete();
    }
}
