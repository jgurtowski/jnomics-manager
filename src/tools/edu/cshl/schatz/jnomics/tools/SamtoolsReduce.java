package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.io.ThreadedLineOperator;
import edu.cshl.schatz.jnomics.io.ThreadedStreamConnector;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer;
import edu.cshl.schatz.jnomics.ob.SAMRecordWritable;
import edu.cshl.schatz.jnomics.util.Functional.Operation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.*;

public class SamtoolsReduce extends JnomicsReducer<SamtoolsMap.SamtoolsKey, SAMRecordWritable, Text, NullWritable> {

    private Throwable readerErr;
    private int reduceIt, binsize;

    private final JnomicsArgument samtools_bin_arg = new JnomicsArgument("samtools_bin","Samtools Binary",true);
    private final JnomicsArgument bcftools_bin_arg = new JnomicsArgument("bcftools_bin","bcftools Binary", true);
    private final JnomicsArgument samtools_opts_arg = new JnomicsArgument("samtools_opts","Samtools mpileup options", false);
    private final JnomicsArgument bcftools_opts_arg = new JnomicsArgument("bcftools_opts","bcftools view options", false);
    private final JnomicsArgument reference_file_arg = new JnomicsArgument("reference_fa",
									   "Reference Fasta (must be indexed)", true);
    

    @Override
    public Class getOutputKeyClass() {
        return Text.class;
    }

    @Override
    public Class getOutputValueClass() {
        return NullWritable.class;
    }

    @Override
    public Class<? extends WritableComparator> getGrouperClass(){
        return SamtoolsReduce.SamtoolsGrouper.class;
    }

    @Override
    public Class<? extends Partitioner> getPartitionerClass(){
        return SamtoolsReduce.SamtoolsPartitioner.class;
    }

    @Override
    public JnomicsArgument[] getArgs() {
        return new JnomicsArgument[]{samtools_bin_arg,bcftools_bin_arg,reference_file_arg,samtools_opts_arg,bcftools_opts_arg};
    }

    public static class SamtoolsGrouper extends WritableComparator {

        public SamtoolsGrouper(){
            super(SamtoolsMap.SamtoolsKey.class,true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b){
            SamtoolsMap.SamtoolsKey first = (SamtoolsMap.SamtoolsKey)a;

            SamtoolsMap.SamtoolsKey second = (SamtoolsMap.SamtoolsKey)b;
            int diff;
            if((diff=first.getRef().compareTo(second.getRef())) == 0)
                diff =first.getBin().compareTo(second.getBin());
            return diff;
        }
    }


    public static class SamtoolsPartitioner extends Partitioner<SamtoolsMap.SamtoolsKey,SAMRecordWritable> {

        private HashPartitioner<String, NullWritable> partitioner = new HashPartitioner<String, NullWritable>();

        @Override
        public int getPartition(SamtoolsMap.SamtoolsKey samtoolsKey, SAMRecordWritable samRecordWritable, int i) {
            String ref = samtoolsKey.getRef().toString();
            String bin = String.valueOf(samtoolsKey.getBin().get());
            return partitioner.getPartition(ref+"-"+bin,NullWritable.get(), i);
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String binsize_str = context.getConfiguration().get(SamtoolsMap.genome_binsize_arg.getName());
        binsize = binsize_str == null ? SamtoolsMap.DEFAULT_GENOME_BINSIZE : Integer.parseInt(binsize_str);
    }

    @Override
    protected void reduce(SamtoolsMap.SamtoolsKey key, final Iterable<SAMRecordWritable> values, final Context context)
            throws IOException, InterruptedException {

	System.out.println("Begin Samtools Map");
        /**Get Configuration **/
        Configuration conf = context.getConfiguration();
        String samtools_bin = conf.get(samtools_bin_arg.getName());
        String bcftools_bin = conf.get(bcftools_bin_arg.getName());
        String reference_file = conf.get(reference_file_arg.getName());
	String samtools_opts = conf.get(samtools_opts_arg.getName(),"");
	String bcftools_opts = conf.get(bcftools_opts_arg.getName(),"");

	System.out.println("Writing temp bam files");
        /**Setup temp bam file**/
        String taskAttemptId = context.getTaskAttemptID().toString();
        File tmpBam = new File(taskAttemptId+"_"+(reduceIt++)+".bam");

        /**launch sam-to-bam conversion and write entries to process**/
        String samtoolsCvtCmd = String.format("%s view -Sb - -o %s", samtools_bin, tmpBam.getAbsolutePath());
        final Process samtoolsCvtProcess = Runtime.getRuntime().exec(samtoolsCvtCmd);

        //reconnect error stream so we can debug process errors through hadoop
        Thread samtoolsCvtProcessErr = new Thread(new ThreadedStreamConnector(samtoolsCvtProcess.getErrorStream(),System.err));
        samtoolsCvtProcessErr.start();
        /**write the entries to the sam-bam convert process**/
        boolean first =  true;
        PrintWriter writer = new PrintWriter(samtoolsCvtProcess.getOutputStream());

        for(SAMRecordWritable record: values){
            if(first){
                writer.println(record.getTextHeader());
                first = false;
            }
            writer.println(record);
        }
        writer.close();

        samtoolsCvtProcess.waitFor();
        samtoolsCvtProcessErr.join();
        
        /** Index the temp bam**/
        String samtoolsIdxCmd = String.format("%s index %s", samtools_bin, tmpBam.getAbsolutePath());
        final Process samtoolsIdxProcess = Runtime.getRuntime().exec(samtoolsIdxCmd);
        samtoolsIdxProcess.waitFor();

	System.out.println("Running mpileup/bcftools snp operation");
        /** Run mpileup on indexed bam and pipe output to bcftools **/
        int binstart = key.getBin().get() * binsize;
        String samtoolsCmd = String.format("%s mpileup %s -r %s:%d-%d -uf %s %s",
					   samtools_bin, samtools_opts, key.getRef().toString(), 
					   binstart, binstart+binsize-1, reference_file, tmpBam.getAbsolutePath());
        String bcftoolsCmd = String.format("%s view %s -vcg -", bcftools_bin, bcftools_opts);
        final Process samtoolsProcess = Runtime.getRuntime().exec(samtoolsCmd);
        final Process bcftoolsProcess = Runtime.getRuntime().exec(bcftoolsCmd);

        /**connect mpileup output to bcftools input **/
        Thread sambcfLink = new Thread(new ThreadedStreamConnector(
                samtoolsProcess.getInputStream(), bcftoolsProcess.getOutputStream()));

        sambcfLink.start();

        /**reconnect stderr for debugging **/
        Thread samtoolsProcessErr = new Thread(new ThreadedStreamConnector(samtoolsProcess.getErrorStream(),System.err));
        Thread bcftoolsProcessErr = new Thread(new ThreadedStreamConnector(bcftoolsProcess.getErrorStream(),System.err));
        samtoolsProcessErr.start();
        bcftoolsProcessErr.start();

        Thread bcfThread = new Thread(new ThreadedLineOperator(bcftoolsProcess.getInputStream(), new Operation(){
            private final Text line = new Text();
            @Override
            public <T> void performOperation(T data){
                String str = (String)data;
                if(!str.startsWith("#")){
                    line.set((String)data);
                    try{
                        context.write(line,NullWritable.get());
                    }catch(Exception e){
                        readerErr = e;
                    }
                }
            }
        }));

        bcfThread.start();

        /*TODO Add check to see if any threads threw an exception*/

        bcfThread.join();
        if(readerErr != null)
            throw new IOException(readerErr);
        samtoolsProcess.waitFor();
        bcftoolsProcess.waitFor();
        sambcfLink.join();
        samtoolsProcessErr.join();
        bcftoolsProcessErr.join();
        File tmpBamIdx = new File(tmpBam.getAbsolutePath()+".bai");
        tmpBam.delete();
        tmpBamIdx.delete();
	System.out.println("Complete, cleaning up");
    }
}
