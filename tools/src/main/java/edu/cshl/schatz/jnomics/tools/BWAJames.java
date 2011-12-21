package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.OptionBuilder;
import edu.cshl.schatz.jnomics.io.*;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsTool;
import edu.cshl.schatz.jnomics.ob.ReadCollectionWritable;
import edu.cshl.schatz.jnomics.util.Functional.Operation;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.util.LinkedList;
import java.util.List;

public class BWAJames extends JnomicsTool{

	private static final Log LOG = LogFactory.getLog(BWAJames.class);

    private static final String BWA_ALN_OPTS_CONF = "bwa.aln.opts";
    private static final String BWA_SAMPE_OPTS_CONF = "bwa.sampe.opts";
    private static final String BWA_IDX_CONF = "bwa.idx";
	private static final String BWA_BINARY_CONF = "bwa.binary";

	public static void main(String[] args)
			throws Exception{
        int exitCode = JnomicsTool.run(new BWAJames(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {

 		Configuration configuration = getConf();
		Job job = new Job(configuration);

		DistributedCache.createSymlink(configuration);

		job.setMapperClass(BWAJames.BWAMap.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        
        job.setNumReduceTasks(0);
		job.setJarByClass(BWAJames.class);
				
		return job.waitForCompletion(true) ? 0 : 1;
    }
	
	@Override
	public Options getOptions() {
       Options options = super.getOptions();
        OptionBuilder optionBuilder = new OptionBuilder();

        options.addOption(optionBuilder
                .withArgName("bwa-aln-opts")
                .withDescription("Quoted string of options to pass to bwa aln binary")
                .hasArg()
                .create("bwaalnopts"));

        options.addOption(optionBuilder
                .withArgName("bwa-sampe-opts")
                .withDescription("Quoted string of options to pass to bwa sampe binary")
                .hasArg()
                .create("bwasampeopts"));


        options.addOption(optionBuilder
                .withArgName("bwa-idx")
                .withDescription("Name of BWA index")
                .hasArg()
                .isRequired(true)
                .create("bwaidx"));
        
        options.addOption(optionBuilder
                .withArgName("bwa-binary")
                .withDescription("name of BWA binary")
                .hasArg()
                .isRequired(true)
                .create("bwabin"));
                
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
            if (opt.equals("bwaalnopts")) {
                getConf().set(BWA_ALN_OPTS_CONF, o.getValue());
            }else if (opt.equals("bwasampeopts")) {
                getConf().set(BWA_SAMPE_OPTS_CONF, o.getValue());
            }else if (opt.equals("bwaidx")) {
                getConf().set(BWA_IDX_CONF, o.getValue());
            }else if (opt.equals("bwabin")) {
            	getConf().set(BWA_BINARY_CONF, o.getValue());
            }else{
                LOG.warn("Unhandled parameter: " + opt + " " + o.getValue());
            }
        }

        return STATUS_OK;
	}

    public static class BWAMap extends Mapper<ReadCollectionWritable,NullWritable,Text,NullWritable>{

        private File[] tmpFiles;
        private Thread readerThread,connector1,connector2;
        private Thread[] responseThreads;
        private Process[] processes;
        private String[] cmds;

		@Override
		protected void setup(final Context context) throws IOException,InterruptedException {

            String bwa_aln_opts = context.getConfiguration().get(BWA_ALN_OPTS_CONF);
            String bwa_sampe_opts = context.getConfiguration().get(BWA_SAMPE_OPTS_CONF);
            bwa_aln_opts = bwa_aln_opts == null ? "" : bwa_aln_opts;
            bwa_sampe_opts = bwa_sampe_opts == null ? "" : bwa_sampe_opts;
			String bwa_idx = context.getConfiguration().get(BWA_IDX_CONF);
			String bwa_binary = context.getConfiguration().get(BWA_BINARY_CONF);
            if(!new File(bwa_binary).exists())
                throw new IOException("Can't find bwa binary: " + bwa_binary);
		
			String taskAttemptId = context.getTaskAttemptID().toString();

            tmpFiles = new File[]{
                    new NamedFIFO(taskAttemptId + ".1.sai"),
                    new NamedFIFO(taskAttemptId + ".2.sai"),
                    new File(taskAttemptId + ".1.fq"),
                    new File(taskAttemptId + ".2.fq")
            };

            for(File file: tmpFiles){
                file.createNewFile();
                file.deleteOnExit();
            }

            cmds = new String[]{
                    String.format(
                            "%s aln %s %s %s",
                            bwa_binary, bwa_aln_opts, bwa_idx, tmpFiles[2]),

                    String.format(
                            "%s aln %s %s %s",
                            bwa_binary, bwa_aln_opts, bwa_idx, tmpFiles[3]),

                    String.format(
                            "%s sampe %s %s %s %s %s %s",
                            bwa_binary, bwa_sampe_opts, bwa_idx, tmpFiles[0], tmpFiles[1],
                            tmpFiles[2], tmpFiles[3])
            };

		}
		
		
		@Override
		public void run(final Context context) throws IOException, InterruptedException {
            setup(context);

            /** Write Temp Files **/
            BufferedOutputStream tmpWriter1 = new BufferedOutputStream(new FileOutputStream(tmpFiles[2]));
            BufferedOutputStream tmpWriter2 = new BufferedOutputStream(new FileOutputStream(tmpFiles[3]));
            ReadCollectionWritable readCollection;
            while(context.nextKeyValue()){
                readCollection = context.getCurrentKey();

                tmpWriter1.write(readCollection.getRead(0).getFastqString().getBytes());
                tmpWriter2.write(readCollection.getRead(1).getFastqString().getBytes());
            }
            tmpWriter1.close();
            tmpWriter2.close();

            /**Launch Processes **/
            processes = new Process[cmds.length];
            responseThreads = new Thread[cmds.length];
            int idx=0;
            for(String cmd: cmds){
                processes[idx] = Runtime.getRuntime().exec(cmd);
                // Reattach stderr to System.stderr
                responseThreads[idx] = new Thread(new ThreadedStreamConnector(processes[idx].getErrorStream(), System.err));
                responseThreads[idx].start();
                ++idx;
            }

            /** Connect stdout of sai to stdin of sampe **/
            connector1 = new Thread(new ThreadedStreamConnector(processes[0].getInputStream(), new FileOutputStream(tmpFiles[0])));
            connector2 = new Thread(new ThreadedStreamConnector(processes[1].getInputStream(), new FileOutputStream(tmpFiles[1])));
            connector1.start();
            connector2.start();

            /** Read lines from sampe stdout and print them to context **/
            readerThread = new Thread(new ThreadedLineOperator(processes[2].getInputStream(), new Operation(){
                private final Text line = new Text();
                @Override
                public <T> void performOperation(T data){
                    line.set((String)data);
                    try{
                        context.write(line,NullWritable.get());
                    }catch(Exception e){
                        assert(false);
                    }
                }
            }));

            readerThread.start();
            cleanup(context);
		}


		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {

            readerThread.join();
            for(Process p: processes)
                p.waitFor();
            for(Thread rt : responseThreads)
                rt.join();
            connector1.join();
            connector2.join();
            for(File file: tmpFiles)
                file.delete();

        }
	}
}