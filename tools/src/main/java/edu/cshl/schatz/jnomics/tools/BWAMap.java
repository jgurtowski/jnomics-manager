package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.io.NamedFIFO;
import edu.cshl.schatz.jnomics.io.ThreadedStreamConnector;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsMapper;
import edu.cshl.schatz.jnomics.ob.ReadCollectionWritable;
import edu.cshl.schatz.jnomics.ob.SAMRecordWritable;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;


public class BWAMap extends JnomicsMapper<ReadCollectionWritable,NullWritable,SAMRecordWritable,NullWritable> {

    private File[] tmpFiles;
    private Thread readerThread,connector1,connector2;
    private Thread[] responseThreads;
    private Process[] processes;
    private String[] cmds;

    private Throwable readerError = null;

    private final JnomicsArgument bwa_aln_opts_arg = new JnomicsArgument("bwa-aln-opts",
            "Alignment options for BWA Align", false);
    private final JnomicsArgument bwa_sampe_opts_arg = new JnomicsArgument("bwa-sampe-opts",
            "Alignment options for BWA Sampe", false);
    private final JnomicsArgument bwa_idx_arg = new JnomicsArgument("bwa-index", "bwa index location", true);
    private final JnomicsArgument bwa_binary_arg = new JnomicsArgument("bwa-binary", "bwa binary location", true);


    @Override
    protected void setup(final Context context) throws IOException,InterruptedException {

        String bwa_aln_opts = context.getConfiguration().get(bwa_aln_opts_arg.getName());
        String bwa_sampe_opts = context.getConfiguration().get(bwa_sampe_opts_arg.getName());
        bwa_aln_opts = bwa_aln_opts == null ? "" : bwa_aln_opts;
        bwa_sampe_opts = bwa_sampe_opts == null ? "" : bwa_sampe_opts;
        String bwa_idx = context.getConfiguration().get(bwa_idx_arg.getName());
        String bwa_binary = context.getConfiguration().get(bwa_binary_arg.getName());
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

        /** Read lines from bwa stdout and print them to context **/
        readerThread = new Thread( new Runnable() {

            private final SAMRecordWritable writableRecord = new SAMRecordWritable();

            @Override
            public void run() {
                SAMFileReader reader = new SAMFileReader(processes[2].getInputStream());
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
        if(readerError != null){
            System.err.println("Error Reading BWA sampe Output with SAM Record Reader");
            throw new IOException(readerError);
        }
        for(Process p: processes)
            p.waitFor();
        for(Thread rt : responseThreads)
            rt.join();
        connector1.join();
        connector2.join();
        for(File file: tmpFiles)
            file.delete();

    }

    @Override
    public Class getOutputKeyClass() {
        return Text.class;
    }

    @Override
    public Class getOutputValueClass() {
        return NullWritable.class;
    }

    @Override
    public JnomicsArgument[] getArgs() {
        return new JnomicsArgument[]{bwa_aln_opts_arg,bwa_binary_arg,bwa_idx_arg,bwa_sampe_opts_arg};
    }
}