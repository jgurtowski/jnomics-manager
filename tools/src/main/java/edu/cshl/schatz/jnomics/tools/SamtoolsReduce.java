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

import java.io.IOException;
import java.io.PrintWriter;

public class SamtoolsReduce extends JnomicsReducer<SamtoolsMap.SamtoolsKey, SAMRecordWritable, Text, NullWritable> {

    private String samtoolsCmd;
    private String bcftoolsCmd;
    private String samtoolsCvtCmd;
    private Throwable readerErr;
    
    private final JnomicsArgument samtools_bin_arg = new JnomicsArgument("samtools_bin","Samtools Binary",true);
    private final JnomicsArgument bcftools_bin_arg = new JnomicsArgument("bcftools_bin","bcftools Binary", true);
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
    public Class getGrouperClass(){
        return SamtoolsReduce.SamtoolsGrouper.class;
    }

    @Override
    public Class getPartitionerClass(){
        return SamtoolsReduce.SamtoolsPartitioner.class;
    }

    @Override
    public JnomicsArgument[] getArgs() {
        return new JnomicsArgument[]{samtools_bin_arg,bcftools_bin_arg,reference_file_arg};
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
        Configuration conf = context.getConfiguration();
        String samtools_bin = conf.get(samtools_bin_arg.getName());
        String bcftools_bin = conf.get(bcftools_bin_arg.getName());
        String reference_file = conf.get(reference_file_arg.getName());

        samtoolsCvtCmd = String.format("%s view -Su -", samtools_bin);
        samtoolsCmd = String.format("%s mpileup -uf %s -", samtools_bin, reference_file);
        bcftoolsCmd = String.format("%s view -vcg -", bcftools_bin);
    }

    @Override
    protected void reduce(SamtoolsMap.SamtoolsKey key, final Iterable<SAMRecordWritable> values, final Context context) throws IOException, InterruptedException {

        final Process samtoolsCvtProcess = Runtime.getRuntime().exec(samtoolsCvtCmd);
        final Process samtoolsProcess = Runtime.getRuntime().exec(samtoolsCmd);
        final Process bcftoolsProcess = Runtime.getRuntime().exec(bcftoolsCmd);


        Thread samcvtLink = new Thread(new ThreadedStreamConnector(
                samtoolsCvtProcess.getInputStream(), samtoolsProcess.getOutputStream()));

        Thread sambcfLink = new Thread(new ThreadedStreamConnector(
                samtoolsProcess.getInputStream(), bcftoolsProcess.getOutputStream()));


        samcvtLink.start();
        sambcfLink.start();

        //reconnect error stream so we can debug process errors through hadoop
        Thread samtoolsCvtProcessErr = new Thread(new ThreadedStreamConnector(samtoolsCvtProcess.getErrorStream(),System.err));
        Thread samtoolsProcessErr = new Thread(new ThreadedStreamConnector(samtoolsProcess.getErrorStream(),System.err));
        Thread bcftoolsProcessErr = new Thread(new ThreadedStreamConnector(bcftoolsProcess.getErrorStream(),System.err));


        samtoolsCvtProcessErr.start();
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

        Thread samWriter = new Thread(new Runnable() {
            @Override
            public void run() {
                boolean first=true;
                PrintWriter writer = new PrintWriter(samtoolsCvtProcess.getOutputStream());
                for(SAMRecordWritable record : values){
                    if(first){
                        writer.println(record.getTextHeader());
                        first = false;
                    }
                    writer.println(record.toString());
                }
                writer.close();
            }
        });

        samWriter.start();

        /*TODO Add check to see if any threads threw an exception*/

        bcfThread.join();
        if(readerErr != null)
            throw new IOException(readerErr);
        samWriter.join();
        samcvtLink.join();
        sambcfLink.join();
        samtoolsCvtProcessErr.join();
        samtoolsProcessErr.join();
        bcftoolsProcessErr.join();
        samtoolsProcess.waitFor();
        bcftoolsProcess.waitFor();
        samtoolsCvtProcess.waitFor();
    }

}