package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.io.ThreadedStreamConnector;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsCounter;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsMapper;
import edu.cshl.schatz.jnomics.ob.ReadCollectionWritable;
import edu.cshl.schatz.jnomics.ob.SAMRecordWritable;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;


/**
 * Runs Bowtie2 Mapping process. Input must be ReadCollectionWritable
 */

public class Bowtie2Map extends JnomicsMapper<ReadCollectionWritable,NullWritable,SAMRecordWritable,NullWritable> {

    private File[] tmpFiles;
    private Thread readerThread, bowtieProcessErrThread;
    private Process bowtieProcess;
    private String cmd;
    private Throwable readerError = null;

    private final JnomicsArgument bowtie_opts = new JnomicsArgument("bowtie_opts", "bowtie options",false);
    private final JnomicsArgument bowtie_idx = new JnomicsArgument("bowtie_index", "bowtie index",true);
    private final JnomicsArgument bowtie_binary = new JnomicsArgument("bowtie_binary", "bowtie binary",true);

    @Override
    public Class getOutputKeyClass(){
        return SAMRecordWritable.class;
    }

    @Override
    public Class getOutputValueClass(){
        return NullWritable.class;
    }
    
    @Override
    public JnomicsArgument[] getArgs(){
        return new JnomicsArgument[]{bowtie_opts,bowtie_idx,bowtie_binary};
    }

    @Override
    protected void setup(final Context context) throws IOException,
            InterruptedException {
        String bowtie_opts_str = context.getConfiguration().get(bowtie_opts.getName());
        bowtie_opts_str = bowtie_opts_str == null ? "" : bowtie_opts_str;
        String bowtie_idx_str = context.getConfiguration().get(bowtie_idx.getName());
        String bowtie_binary_str = context.getConfiguration().get(bowtie_binary.getName());
        if(!new File(bowtie_binary_str).exists())
            throw new IOException("Can't find bowtie binary: " + bowtie_binary_str);

        String taskAttemptId = context.getTaskAttemptID().toString();

        //write reads to temp files and then call Bowtie2 on the temp files
        //Bowtie2 does not like pipes
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
                bowtie_binary_str, bowtie_opts_str, bowtie_idx_str,
                tmpFiles[0],
                tmpFiles[1]);

    }

    @Override
    public void run(final Context context) throws IOException, InterruptedException {
        setup(context);

        System.err.println("Writing Temp Files");
        
        /** Write Temp Files **/
        BufferedOutputStream tmpWriter1 = new BufferedOutputStream(new FileOutputStream(tmpFiles[0]));
        BufferedOutputStream tmpWriter2 = new BufferedOutputStream(new FileOutputStream(tmpFiles[1]));
        ReadCollectionWritable readCollection;
        while(context.nextKeyValue()){
            readCollection = context.getCurrentKey();
            tmpWriter1.write(readCollection.getRead(0).getFastqString().getBytes());
            tmpWriter1.write("\n".getBytes());
            tmpWriter2.write(readCollection.getRead(1).getFastqString().getBytes());
            tmpWriter2.write("\n".getBytes());
        }
        tmpWriter1.close();
        tmpWriter2.close();

        System.err.println("Starting bowtie2 Process");
        bowtieProcess = Runtime.getRuntime().exec(cmd);
        bowtieProcessErrThread = new Thread(new ThreadedStreamConnector(bowtieProcess.getErrorStream(), System.err));
        bowtieProcessErrThread.start();

        /** Read lines from bowtie stdout and print them to context **/
        readerThread = new Thread( new Runnable() {

            private final SAMRecordWritable writableRecord = new SAMRecordWritable();

            @Override
            public void run() {
                SAMFileReader reader = new SAMFileReader(bowtieProcess.getInputStream());
                reader.setValidationStringency(SAMFileReader.ValidationStringency.LENIENT);
                Counter mapped_counter = context.getCounter(JnomicsCounter.Alignment.MAPPED);
                Counter totalreads_counter = context.getCounter(JnomicsCounter.Alignment.TOTAL);
                for(SAMRecord record: reader){
                    writableRecord.set(record);
                    totalreads_counter.increment(1);
                    if(writableRecord.getMappingQuality().get() != 0)
                        mapped_counter.increment(1);
                    try {
                        context.write(writableRecord,NullWritable.get());
                        context.progress();
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
        if(readerError != null){
            System.err.println("Error Reading Bowtie Output with SAM Record Reader");
            throw new IOException(readerError);
        }
        bowtieProcess.waitFor();
        bowtieProcessErrThread.join();
        System.err.println("Cleaning UP");
        for(File f: tmpFiles){
            f.delete();
        }
        System.err.println("done");
    }

}