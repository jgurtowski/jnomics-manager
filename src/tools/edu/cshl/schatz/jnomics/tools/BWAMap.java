package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.io.ThreadedStreamConnector;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsCounter;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsMapper;
import edu.cshl.schatz.jnomics.ob.*;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;


public class BWAMap extends JnomicsMapper<Writable,NullWritable,AlignmentCollectionWritable,NullWritable> {

    private File[] tmpFiles;
    private String[] aln_cmds_pair,aln_cmds_single;
    private String sampe_cmd, samse_cmd;
    private Process process;

    private Throwable readerError = null;

    private final JnomicsArgument bwa_aln_opts_arg = new JnomicsArgument("bwa_aln_opts",
            "Alignment options for BWA Align", false);
    private final JnomicsArgument bwa_sampe_opts_arg = new JnomicsArgument("bwa_sampe_opts",
            "Alignment options for BWA Sampe", false);
    private final JnomicsArgument bwa_samse_opts_arg = new JnomicsArgument("bwa_samse_opts",
            "Alignment options for BWA Samse", false);
    private final JnomicsArgument bwa_idx_arg = new JnomicsArgument("bwa_index", "bwa index location", true);
    private final JnomicsArgument bwa_binary_arg = new JnomicsArgument("bwa_binary", "bwa binary location", true);


    @Override
    protected void setup(final Context context) throws IOException,InterruptedException {

        String bwa_aln_opts = context.getConfiguration().get(bwa_aln_opts_arg.getName(),"");
        String bwa_sampe_opts = context.getConfiguration().get(bwa_sampe_opts_arg.getName(), "");
        String bwa_samse_opts = context.getConfiguration().get(bwa_samse_opts_arg.getName(),"");
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
        aln_cmds_pair = new String[]{
                String.format(
                        "%s aln %s %s %s",
                        bwa_binary, bwa_aln_opts, bwa_idx, tmpFiles[2]),

                String.format(
                        "%s aln %s %s %s",
                        bwa_binary, bwa_aln_opts, bwa_idx, tmpFiles[3])

        };

        aln_cmds_single = new String[]{aln_cmds_pair[0]};
        
        sampe_cmd = String.format(
                "%s sampe %s %s %s %s %s %s",
                bwa_binary, bwa_sampe_opts, bwa_idx, tmpFiles[0], tmpFiles[1],
                tmpFiles[2], tmpFiles[3]
        );

        samse_cmd = String.format(
                "%s samse %s %s %s %s",
                bwa_binary, bwa_samse_opts, bwa_idx, tmpFiles[0],  tmpFiles[2]
        );

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
        SudoCollection<FastqStringProvider> collection;
        List<ReadWritable> reads;
        boolean first=true, paired=false;
        while(context.nextKeyValue()){
            collection = (SudoCollection<FastqStringProvider>) context.getCurrentKey();
            tmpWriter1.write(collection.get(0).getFastqString().getBytes());
            tmpWriter1.write("\n".getBytes());
            if(collection.size() == 2){
                if(first)
                    paired=true;
                tmpWriter2.write(collection.get(1).getFastqString().getBytes());
                tmpWriter2.write("\n".getBytes());
            }
        }
        tmpWriter1.close();
        tmpWriter2.close();

        Thread connecterr,connectout;

        System.out.println("launching alignment");
        
        /**Launch Processes **/
        int idx = 0;
        FileOutputStream fout;

        for(String cmd: paired ? aln_cmds_pair : aln_cmds_single){
            process = Runtime.getRuntime().exec(cmd);
            System.out.println(cmd);
            // Reattach stderr and write System.stdout to tmp file
            connecterr = new Thread(
                    new ThreadedStreamConnector(process.getErrorStream(), System.err){
                        @Override
                        public void progress() {
                            context.progress();
                        }
                    });
            fout = new FileOutputStream(tmpFiles[idx]);
            connectout = new Thread(new ThreadedStreamConnector(process.getInputStream(),fout));
            connecterr.start();connectout.start();
            connecterr.join();connectout.join();
            process.waitFor();
            fout.close();
            idx++;
            context.progress();
        }

        System.out.println("running sampe/samse");
        
        /**Launch sampe command*/
        final Process sam_process = Runtime.getRuntime().exec(paired ? sampe_cmd : samse_cmd);

        connecterr = new Thread(new ThreadedStreamConnector(sam_process.getErrorStream(), System.err));
        connecterr.start();
        
        final boolean pe = paired;
        /** setup reader thread - reads lines from bwa stdout and print them to context **/
        Thread readerThread = new Thread( new Runnable() {

            private final AlignmentCollectionWritable alignmentCollection = new AlignmentCollectionWritable();
            {
                alignmentCollection.addAlignment(new SAMRecordWritable());
                if(pe){
                    alignmentCollection.addAlignment(new SAMRecordWritable());
                }
            }
            
            @Override
            public void run() {
                SAMFileReader reader = new SAMFileReader(sam_process.getInputStream());
                reader.setValidationStringency(SAMFileReader.ValidationStringency.LENIENT);
                Counter mapped_counter = context.getCounter(JnomicsCounter.Alignment.MAPPED);
                Counter totalreads_counter = context.getCounter(JnomicsCounter.Alignment.TOTAL);
                int i = 0;
                for(SAMRecord record: reader){
                    if(i > 1 )
                        System.out.print("I is : " + i);
                    alignmentCollection.getAlignment(i).set(record);
                    totalreads_counter.increment(1);
                    if(2 == (2 & record.getFlags()))
                        mapped_counter.increment(1);
                    try {
                        if((!pe) || (i >= 1) ){
                            context.write(alignmentCollection,NullWritable.get());
                            context.progress();
                            i=0;
                        }else{
                            i++;
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
        sam_process.waitFor();
        connecterr.join();

        System.out.println("removing tmp files");
        for(File file: tmpFiles)
            file.delete();

        if(readerError != null){
            System.err.println("Error Reading BWA sampe/samse Output with SAM Record Reader");
            throw new IOException(readerError);
        }
    }


    @Override
    public Class getOutputKeyClass() {
        return AlignmentCollectionWritable.class;
    }

    @Override
    public Class getOutputValueClass() {
        return NullWritable.class;
    }

    @Override
    public JnomicsArgument[] getArgs() {
        return new JnomicsArgument[]{bwa_aln_opts_arg,bwa_binary_arg,bwa_idx_arg,bwa_sampe_opts_arg,bwa_samse_opts_arg};
    }
}