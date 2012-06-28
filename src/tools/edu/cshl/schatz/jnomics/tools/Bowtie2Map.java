package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.io.ThreadedStreamConnector;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsCounter;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsMapper;
import edu.cshl.schatz.jnomics.ob.AlignmentCollectionWritable;
import edu.cshl.schatz.jnomics.ob.FastqStringProvider;
import edu.cshl.schatz.jnomics.ob.SAMRecordWritable;
import edu.cshl.schatz.jnomics.ob.SudoCollection;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;


/**
 * Runs Bowtie2 Mapping process. Input must be ReadCollectionWritable
 */

public class Bowtie2Map extends JnomicsMapper<Writable,NullWritable,AlignmentCollectionWritable,NullWritable> {

    private File[] tmpFiles;
    private Thread readerThread, bowtieProcessErrThread;
    private Process bowtieProcess;
    private String cmdPairedEnd,cmdSingleEnd;
    private Throwable readerError = null;

    private final JnomicsArgument bowtie_opts = new JnomicsArgument("bowtie_opts", "bowtie options",false);
    private final JnomicsArgument bowtie_idx = new JnomicsArgument("bowtie_index", "bowtie index",true);
    private final JnomicsArgument bowtie_binary = new JnomicsArgument("bowtie_binary", "bowtie binary",true);

    @Override
    public Class getOutputKeyClass(){
        return AlignmentCollectionWritable.class;
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

        cmdPairedEnd = String.format(
                "%s %s --mm -x %s -1 %s -2 %s",
                bowtie_binary_str, bowtie_opts_str, bowtie_idx_str,
                tmpFiles[0],
                tmpFiles[1]);

        cmdSingleEnd = String.format("%s %s --mm -x %s -U %s",
                bowtie_binary_str,
                bowtie_opts_str,
                bowtie_idx_str,
                tmpFiles[0]);
    }

    @Override
    public void run(final Context context) throws IOException, InterruptedException {
        setup(context);

        /** Write Temp Files **/
        BufferedOutputStream tmpWriter1 = new BufferedOutputStream(new FileOutputStream(tmpFiles[0]));
        BufferedOutputStream tmpWriter2 = new BufferedOutputStream(new FileOutputStream(tmpFiles[1]));

        SudoCollection<FastqStringProvider> collection;
        boolean first = true;
        boolean pairedEnd = false;
        while(context.nextKeyValue()){
            collection = (SudoCollection<FastqStringProvider>) context.getCurrentKey();
            if(first){
                if(collection.size() == 2)
                    pairedEnd = true;
                first = false;
            }
            if(pairedEnd){
                tmpWriter1.write(collection.get(0).getFastqString().getBytes());
                tmpWriter1.write("\n".getBytes());
                tmpWriter2.write(collection.get(1).getFastqString().getBytes());
                tmpWriter2.write("\n".getBytes());
            }else{

                tmpWriter1.write(collection.get(0).getFastqString().getBytes());
                tmpWriter1.write("\n".getBytes());
            }

        }

        tmpWriter1.close();
        tmpWriter2.close();

        System.err.println("Starting bowtie2 Process");
        if(pairedEnd){
            bowtieProcess = Runtime.getRuntime().exec(cmdPairedEnd);
            System.out.println(cmdPairedEnd);
        }else{
            bowtieProcess = Runtime.getRuntime().exec(cmdSingleEnd);
            System.out.println(cmdSingleEnd);
        }
        bowtieProcessErrThread = new Thread(new ThreadedStreamConnector(bowtieProcess.getErrorStream(), System.err));
        bowtieProcessErrThread.start();

        /** Read lines from bowtie stdout and print them to context **/
        final boolean pe = pairedEnd;
        readerThread = new Thread( new Runnable() {

            private final AlignmentCollectionWritable collectionWritable = new AlignmentCollectionWritable();

            {
                collectionWritable.addAlignment(new SAMRecordWritable());
                if(pe)
                    collectionWritable.addAlignment(new SAMRecordWritable());
            }

            @Override
            public void run() {
                SAMFileReader reader = new SAMFileReader(bowtieProcess.getInputStream());
                reader.setValidationStringency(SAMFileReader.ValidationStringency.LENIENT);
                Counter mapped_counter = context.getCounter(JnomicsCounter.Alignment.MAPPED);
                Counter totalreads_counter = context.getCounter(JnomicsCounter.Alignment.TOTAL);
                int idx = 0;
                for(SAMRecord record: reader){
                    collectionWritable.getAlignment(idx).set(record);
                    totalreads_counter.increment(1);

                    if(2 == (2 & record.getFlags()))
                        mapped_counter.increment(1);
                    try {
                        if(!pe || 1 == idx++){
                            context.write(collectionWritable,NullWritable.get());
                            context.progress();
                            idx = 0;
                        }
                    }catch(Exception e){
                        readerError = e;
                    }
                }
                reader.close();
            }
        });

        readerThread.start();

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