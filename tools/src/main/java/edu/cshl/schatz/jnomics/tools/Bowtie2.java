package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.OptionBuilder;
import edu.cshl.schatz.jnomics.io.ThreadedStreamConnector;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsTool;
import edu.cshl.schatz.jnomics.ob.ReadCollectionWritable;
import edu.cshl.schatz.jnomics.ob.SAMRecordWritable;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class Bowtie2 extends JnomicsTool{

	private static final Log LOG = LogFactory.getLog(Bowtie2.class);
	
	private static final String BOWTIE_OPTS_CONF = "bowtie2.opts";	
	private static final String BOWTIE_IDX_CONF = "bowtie2.idx";	
	private static final String BOWTIE_BINARY_CONF = "bowtie2.binary";	

	public static void main(String[] args) 
			throws Exception{
		
		//int exitCode = ToolRunner.run(new Bowtie2(), args);
		int exitCode = JnomicsTool.run(new Bowtie2(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception {
		
        Configuration configuration = getConf();
        Job job = new Job(configuration);

		DistributedCache.createSymlink(configuration);

        job.setMapperClass(Bowtie2.Bowtie2Map.class);
        job.setReducerClass(Reducer.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(SAMRecordWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(SAMRecordWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);

        job.setJarByClass(Bowtie2.class);

		return job.waitForCompletion(true) ? 0 : 1;
    }
    
	@Override
	public Options getOptions() {
       Options options = super.getOptions();
        OptionBuilder optionBuilder = new OptionBuilder();
        
        options.addOption(optionBuilder
                .withArgName("bowtie-opts")
                .withDescription("Quoted string of options to pass to bowtie binary")
                .hasArg()
                .create("bowtieopts"));
        
        options.addOption(optionBuilder
                .withArgName("bowtie-idx")
                .withDescription("Name of Bowtie index")
                .hasArg()
                .isRequired(true)
                .create("bowtieidx"));
        
        options.addOption(optionBuilder
                .withArgName("bowtie-binary")
                .withDescription("name of bowtie binary")
                .hasArg()
                .isRequired(true)
                .create("bowtiebin"));
                
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
            if (opt.equals("bowtieopts")) {
                getConf().set(BOWTIE_OPTS_CONF, o.getValue());
            }else if (opt.equals("bowtieidx")) {
            	getConf().set(BOWTIE_IDX_CONF, o.getValue());
            }else if (opt.equals("bowtiebin")) {
            	getConf().set(BOWTIE_BINARY_CONF, o.getValue());
            }else{
                LOG.warn("Unhandled parameter: " + opt + " " + o.getValue());
            }
        }

        return STATUS_OK;
	}

    public static class Bowtie2Map extends Mapper<ReadCollectionWritable,NullWritable,SAMRecordWritable,NullWritable> {

		private File[] tmpFiles;
		private Thread readerThread, bowtieProcessErrThread;
		private Process bowtieProcess;
        private String cmd;
        private Throwable readerError = null;

        @Override
		protected void setup(final Context context) throws IOException,
											InterruptedException {
			String bowtie_opts = context.getConfiguration().get(BOWTIE_OPTS_CONF);
			bowtie_opts = bowtie_opts == null ? "" : bowtie_opts;
			String bowtie_idx = context.getConfiguration().get(BOWTIE_IDX_CONF);
			String bowtie_binary = context.getConfiguration().get(BOWTIE_BINARY_CONF);
			if(!new File(bowtie_binary).exists())
				throw new IOException("Can't find bowtie binary: " + bowtie_binary);
		
			String taskAttemptId = context.getTaskAttemptID().toString();
            tmpFiles = new File[]{
                    new File(taskAttemptId+".1.fq"),
                    new File(taskAttemptId+".2.fq")
            };

            for(File f: tmpFiles){
                f.createNewFile();
                f.deleteOnExit();
            }

            cmd = String.format(
                    "%s %s --mm -x %s -1 %s -2 %s",
                    bowtie_binary, bowtie_opts, bowtie_idx,
        			tmpFiles[0],
        			tmpFiles[1]);

		}

		@Override
		public void run(final Context context) throws IOException, InterruptedException {
			setup(context);

            /** Write Temp Files **/
            BufferedOutputStream tmpWriter1 = new BufferedOutputStream(new FileOutputStream(tmpFiles[0]));
            BufferedOutputStream tmpWriter2 = new BufferedOutputStream(new FileOutputStream(tmpFiles[1]));
            ReadCollectionWritable readCollection;
            while(context.nextKeyValue()){
                readCollection = context.getCurrentKey();

                tmpWriter1.write(readCollection.getRead(0).getFastqString().getBytes());
                tmpWriter2.write(readCollection.getRead(1).getFastqString().getBytes());
            }
            tmpWriter1.close();
            tmpWriter2.close();


            bowtieProcess = Runtime.getRuntime().exec(cmd);
            bowtieProcessErrThread = new Thread(new ThreadedStreamConnector(bowtieProcess.getErrorStream(), System.err));
            bowtieProcessErrThread.start();

            /** Read lines from bowtie stdout and print them to context **/
            readerThread = new Thread( new Runnable() {

                private final SAMRecordWritable writableRecord = new SAMRecordWritable();
                
                @Override
                public void run() {
                    SAMFileReader reader = new SAMFileReader(bowtieProcess.getInputStream());
                    for(SAMRecord record: reader){
                        writableRecord.set(record);
                        try {
                            context.write(writableRecord,NullWritable.get());
                        }catch(Exception e){
                            readerError = e;
                        }
                    }
                    reader.close();
                }
            });

            readerThread.start();

			cleanup(context);
		}


		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			
            readerThread.join();
            if(readerError != null)
                throw new IOException(readerError);

            bowtieProcess.waitFor();
            bowtieProcessErrThread.join();

            for(File f: tmpFiles){
                f.delete();
            }
		}

	}
			
}