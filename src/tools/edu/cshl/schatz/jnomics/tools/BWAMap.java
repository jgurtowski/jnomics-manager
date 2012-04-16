package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.io.ThreadedStreamConnector;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsMapper;
import edu.cshl.schatz.jnomics.ob.ReadCollectionWritable;
import edu.cshl.schatz.jnomics.ob.SAMRecordWritable;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;
import org.apache.hadoop.io.NullWritable;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;


public class BWAMap extends JnomicsMapper<ReadCollectionWritable,NullWritable,SAMRecordWritable,NullWritable> {

    private File[] tmpFiles;
    private String[] aln_cmds;
    private String sampe_cmd;
    private Process process;

    private Throwable readerError = null;

    private final JnomicsArgument bwa_aln_opts_arg = new JnomicsArgument("bwa_aln_opts",
            "Alignment options for BWA Align", false);
    private final JnomicsArgument bwa_sampe_opts_arg = new JnomicsArgument("bwa_sampe_opts",
            "Alignment options for BWA Sampe", false);
    private final JnomicsArgument bwa_idx_arg = new JnomicsArgument("bwa_index", "bwa index location", true);
    private final JnomicsArgument bwa_binary_arg = new JnomicsArgument("bwa_binary", "bwa binary location", true);


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
                new File(taskAttemptId + ".1.sai"),
                new File(taskAttemptId + ".2.sai"),
                new File(taskAttemptId + ".1.fq"),
                new File(taskAttemptId + ".2.fq")
        };

        for(File file: tmpFiles){
            file.createNewFile();
            file.deleteOnExit();
        }
        System.out.println("BWA index--->"+bwa_idx);
        aln_cmds = new String[]{
                String.format(
                        "%s aln %s %s %s",
                        bwa_binary, bwa_aln_opts, bwa_idx, tmpFiles[2]),

                String.format(
                        "%s aln %s %s %s",
                        bwa_binary, bwa_aln_opts, bwa_idx, tmpFiles[3])

        };

        sampe_cmd = String.format(
                "%s sampe %s %s %s %s %s %s",
                bwa_binary, bwa_sampe_opts, bwa_idx, tmpFiles[0], tmpFiles[1],
                tmpFiles[2], tmpFiles[3]);

        
    }


    @Override
    public void run(final Context context) throws IOException, InterruptedException {
        setup(context);

        /** Write Temp Files **/
        BufferedOutputStream tmpWriter1 = new BufferedOutputStream(new FileOutputStream(tmpFiles[2]));
        BufferedOutputStream tmpWriter2 = new BufferedOutputStream(new FileOutputStream(tmpFiles[3]));
        System.out.println("Tmp Files----->");
        System.out.println(tmpFiles[2]);
        System.out.println(tmpFiles[3]);
        System.out.println("-----<");
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


        Thread connecterr,connectout;

        System.out.println("launching alignment");
        
        /**Launch Processes **/
        int idx = 0;
        FileOutputStream fout;
        for(String cmd: aln_cmds){
            process = Runtime.getRuntime().exec(cmd);
            System.out.println(cmd);
            // Reattach stderr and write System.stdout to tmp file
            connecterr = new Thread(new ThreadedStreamConnector(process.getErrorStream(), System.err));
            fout = new FileOutputStream(tmpFiles[idx]);
            connectout = new Thread(new ThreadedStreamConnector(process.getInputStream(),fout));
            connecterr.start();connectout.start();
            connecterr.join();connectout.join();
            process.waitFor();
            fout.close();
            idx++;
            context.progress();
        }

        System.out.println("running sampe");
        
        /**Launch sampe command*/
        final Process sampe_process = Runtime.getRuntime().exec(sampe_cmd);

        connecterr = new Thread(new ThreadedStreamConnector(sampe_process.getErrorStream(), System.err));
        connecterr.start();
        /** setup reader thread - reads lines from bwa stdout and print them to context **/
        Thread readerThread = new Thread( new Runnable() {

            private final SAMRecordWritable writableRecord = new SAMRecordWritable();

            @Override
            public void run() {
                SAMFileReader reader = new SAMFileReader(sampe_process.getInputStream());
                reader.setValidationStringency(SAMFileReader.ValidationStringency.LENIENT);
                for(SAMRecord record: reader){
                    writableRecord.set(record);
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
        readerThread.join();
        sampe_process.waitFor();
        connecterr.join();
        

        System.out.println("deleteing tmp files");
        for(File file: tmpFiles)
            file.delete();

        if(readerError != null){
            System.err.println("Error Reading BWA sampe Output with SAM Record Reader");
            throw new IOException(readerError);
        }
    }


    @Override
    public Class getOutputKeyClass() {
        return SAMRecordWritable.class;
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