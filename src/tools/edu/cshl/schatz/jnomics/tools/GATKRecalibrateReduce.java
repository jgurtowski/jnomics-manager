package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.io.ThreadedStreamConnector;
import edu.cshl.schatz.jnomics.ob.SAMRecordWritable;
import edu.cshl.schatz.jnomics.util.ProcessUtil;
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
public class GATKRecalibrateReduce extends GATKBaseReduce<SamtoolsMap.SamtoolsKey,SAMRecordWritable,NullWritable,NullWritable> {

    private JnomicsArgument vcf_mask_arg = new JnomicsArgument("vcf_mask","VCF file to mask known snps/indels",true);
    private File vcf_mask;
    
    private FileSystem fs;
    
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
    public JnomicsArgument[] getArgs() {
        JnomicsArgument[] args = super.getArgs();
        JnomicsArgument[] newArgs = new JnomicsArgument[args.length+1];
        newArgs[0] = vcf_mask_arg;
        System.arraycopy(args,0,newArgs,1,args.length);
        return newArgs;
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        vcf_mask = binaries.get(vcf_mask_arg.getName());
        fs = FileSystem.get(context.getConfiguration());
    }

    @Override
    protected void reduce(SamtoolsMap.SamtoolsKey key, Iterable<SAMRecordWritable> alignments, Context context)
            throws IOException, InterruptedException {

        File tmp_bam = new File(context.getTaskAttemptID()+".tmp.bam");
        /**Write bam to temp file**/
        String samtools_convert_cmd = String.format("%s view -Sb -o %s -", samtools_binary, tmp_bam);
        System.out.println(samtools_convert_cmd);
        Process samtools_convert = Runtime.getRuntime().exec(samtools_convert_cmd);

        Thread errConn = new Thread(new ThreadedStreamConnector(samtools_convert.getErrorStream(),System.err));
        Thread outConn = new Thread(new ThreadedStreamConnector(samtools_convert.getInputStream(),System.out));
        outConn.start();errConn.start();

        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(samtools_convert.getOutputStream()));
        long count = 0;
        for(SAMRecordWritable record: alignments){
            if(0 == count){
                writer.write(record.getTextHeader()+"\n");
            }
            writer.write(record+"\n");
            if(0 == ++count % 1000 ){
                context.progress();
                context.write(NullWritable.get(),NullWritable.get());
            }
        }
        writer.close();
        samtools_convert.waitFor();
        outConn.join();errConn.join();

        /**Index Bam**/
        String samtools_idx_cmd = String.format("%s index %s", samtools_binary, tmp_bam);
        ProcessUtil.exceptionOnError(ProcessUtil.execAndReconnect(samtools_idx_cmd));

        /** Count Covars **/
        File recal_out = new File(key.getRef()+"-"+key.getBin()+".covar");
        String countCovars = String.format("java -Xmx4g -jar %s -T CountCovariates -R %s -I %s -knownSites %s -cov ReadGroupCovariate -cov QualityScoreCovariate -cov CycleCovariate -cov DinucCovariate -recalFile %s",
                gatk_binary,reference_fa,tmp_bam,vcf_mask,recal_out);
        ProcessUtil.exceptionOnError(ProcessUtil.execAndReconnect(countCovars));

        Configuration conf = context.getConfiguration();
        fs.copyFromLocalFile(new Path(recal_out.toString()),new Path(conf.get("mapred.output.dir")));
    }
}
