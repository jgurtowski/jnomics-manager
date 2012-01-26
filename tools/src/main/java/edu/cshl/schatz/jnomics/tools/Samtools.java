package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.OptionBuilder;
import edu.cshl.schatz.jnomics.io.ThreadedLineOperator;
import edu.cshl.schatz.jnomics.io.ThreadedStreamConnector;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsTool;
import edu.cshl.schatz.jnomics.ob.SAMRecordWritable;
import edu.cshl.schatz.jnomics.util.Functional.Operation;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintWriter;

public class Samtools extends JnomicsTool{

	private static final Log LOG = LogFactory.getLog(Samtools.class);

    private static final String BCFTOOLS_BIN_CONF = "samtools.bcftools.bin";
    private static final String SAMTOOLS_BIN_CONF = "samtools.bin";
    private static final String REFERENCE_FA_CONF = "samtools.reference.fa";
    private static final String GENOME_BINSIZE_CONF = "samtools.genome.binsize";

    
	public static void main(String[] args)
			throws Exception{

        int exitCode = JnomicsTool.run(new Samtools(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {

        Configuration configuration = getConf();
        Job job = new Job(configuration);

		DistributedCache.createSymlink(configuration);

        job.setMapperClass(Samtools.SamtoolsMap.class);
        job.setReducerClass(SamtoolsReduce.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

        job.setPartitionerClass(Samtools.SamtoolsPartitioner.class);
        job.setGroupingComparatorClass(Samtools.SamtoolsGrouper.class);

        job.setMapOutputKeyClass(SamtoolsKey.class);
		job.setMapOutputValueClass(SAMRecordWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setJarByClass(Samtools.class);

		return job.waitForCompletion(true) ? 0 : 1;
    }

    private static class SamtoolsGrouper extends WritableComparator{

        public SamtoolsGrouper(){
            super(SamtoolsKey.class,true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b){
            SamtoolsKey first = (SamtoolsKey)a;
            SamtoolsKey second = (SamtoolsKey)b;
            int diff;
            if((diff=first.getRef().compareTo(second.getRef())) == 0)
                diff =first.getBin().compareTo(second.getBin());
            return diff;
        }
    }


    private static class SamtoolsPartitioner extends Partitioner<SamtoolsKey,SAMRecordWritable> {
        
        private HashPartitioner<String, NullWritable> partitioner = new HashPartitioner<String, NullWritable>();

        @Override
        public int getPartition(SamtoolsKey samtoolsKey, SAMRecordWritable samRecordWritable, int i) {
            String ref = samtoolsKey.getRef().toString();
            String bin = String.valueOf(samtoolsKey.getBin().get());
            return partitioner.getPartition(ref+"-"+bin,NullWritable.get(), i);
        }
    }


    public static class SamtoolsKey implements WritableComparable<SamtoolsKey>{

        private Text ref = new Text();
        private IntWritable bin = new IntWritable();
        private IntWritable position = new IntWritable();

        public SamtoolsKey(){}

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            ref.write(dataOutput);
            bin.write(dataOutput);
            position.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            ref.readFields(dataInput);
            bin.readFields(dataInput);
            position.readFields(dataInput);
        }

        @Override
        public int compareTo(SamtoolsKey o) {
            int diff;
            if((diff = ref.compareTo(o.getRef())) != 0)return diff;
            if((diff = bin.compareTo(o.getBin())) != 0)return diff;
            return position.compareTo(o.getPosition());
        }

        public String toString(){
         return ref.toString() +"-" + bin.toString() + "-" +position.toString();
        }
        
        public void setRef(String ref){
            this.ref.set(ref);
        }

        public void setBin(int bin){
            this.bin.set(bin);
        }

        public void setPosition(int position){
            this.position.set(position);
        }

        public void setRef(Text ref){
            this.ref.set(ref);
        }

        public Text getRef() {
            return ref;
        }

        public IntWritable getBin() {
            return bin;
        }

        public IntWritable getPosition() {
            return position;
        }
    }

    @Override
    public Options getOptions() {
        Options options = super.getOptions();
        OptionBuilder optionBuilder = new OptionBuilder();

        options.addOption(optionBuilder
                .withArgName("samtoolsbin")
                .withDescription("samtoolsbin")
                .hasArg()
                .create("samtoolsbin"));

        options.addOption(optionBuilder
                .withArgName("bcfbin")
                .withDescription("bcfbin")
                .hasArg()
                .isRequired(true)
                .create("bcfbin"));

        options.addOption(optionBuilder
                .withArgName("faref")
                .withDescription("reference in fa (indexed)")
                .hasArg()
                .isRequired(true)
                .create("faref"));

        options.addOption(optionBuilder
                .withArgName("genome_binsize")
                .withDescription("size of genome bins")
                .hasArg()
                .isRequired(false)
                .create("genome_binsize"));

        return options;
    }



    @Override
    public int handleOptions(CommandLine cmd) throws ParseException, IOException {
        for (String unrecognizedOption : cmd.getArgs()) {
            if (unrecognizedOption.startsWith("-")) {
                throw new ParseException("Unrecognized option " + unrecognizedOption);
            }
        }

        for (Option o : cmd.getOptions()) {
            String opt = o.getOpt();
            if (opt.equals("samtoolsbin")) {
                getConf().set(SAMTOOLS_BIN_CONF, o.getValue());
            }else if (opt.equals("bcfbin")) {
                getConf().set(BCFTOOLS_BIN_CONF, o.getValue());
            }else if (opt.equals("faref")) {
                getConf().set(REFERENCE_FA_CONF, o.getValue());
            }else if (opt.equals("genome_binsize")) {
                getConf().set(GENOME_BINSIZE_CONF, o.getValue());
            }else{
                LOG.warn("Unhandled parameter: " + opt + " " + o.getValue());
            }
        }

        return STATUS_OK;
    }




    public static class SamtoolsMap extends Mapper<SAMRecordWritable,NullWritable, SamtoolsKey, SAMRecordWritable> {
        
        private final SamtoolsKey stkey = new SamtoolsKey();
        private int binsize;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            binsize = Integer.parseInt(conf.get("GENOME_BINSIZE_CONF","100000"));
        }

        @Override
        protected void map(SAMRecordWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
            if(key.getMappingQuality().get() == 0) // remove unmapped reads
                return;
            int alignmentStart = key.getAlignmentStart().get();
            stkey.setPosition(alignmentStart);
            stkey.setRef(key.getReferenceName());
            int bin = alignmentStart / binsize;
            stkey.setBin(bin);
            context.write(stkey, key);
            int end = alignmentStart + key.getReadString().toString().length();
            if(end >= (bin+1) * binsize){
                stkey.setBin(bin+1);
                context.write(stkey,key);
            }
        }
    }
    
    public static class SamtoolsReduce extends Reducer<SamtoolsKey, SAMRecordWritable, Text, NullWritable>{

        private String samtoolsCmd;
        private String bcftoolsCmd;
        private String samtoolsCvtCmd;
        private Throwable readerErr;
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String samtools_bin = conf.get(SAMTOOLS_BIN_CONF);
            String bcftools_bin = conf.get(BCFTOOLS_BIN_CONF); 
            String reference_file = conf.get(REFERENCE_FA_CONF);

            samtoolsCvtCmd = String.format("%s view -Su -", samtools_bin);
            samtoolsCmd = String.format("%s mpileup -uf %s -", samtools_bin, reference_file);
            bcftoolsCmd = String.format("%s view -vcg -", bcftools_bin);
        }

        @Override
        protected void reduce(SamtoolsKey key, final Iterable<SAMRecordWritable> values, final Context context) throws IOException, InterruptedException {

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

                        writer.println(record);
                        
                    }
                    writer.close();
                }
            });

            samWriter.start();



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

}