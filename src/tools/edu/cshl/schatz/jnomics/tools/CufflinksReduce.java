package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.io.ThreadedStreamConnector;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer;
import edu.cshl.schatz.jnomics.ob.SAMRecordWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * User: james
 */
public class CufflinksReduce extends JnomicsReducer<SamtoolsMap.SamtoolsKey,SAMRecordWritable,NullWritable,NullWritable> {
    
    private final JnomicsArgument cuff_binary_arg = new JnomicsArgument("cufflinks_binary","Cufflinks Binary",true);

    private String cufflinks_cmd;
    
    @Override
    public Class getOutputKeyClass() {
        return NullWritable.class;
    }

    @Override
    public Class getOutputValueClass() {
        return NullWritable.class;
    }

    @Override
    public JnomicsArgument[] getArgs() {
        return new JnomicsArgument[]{cuff_binary_arg};
    }

    @Override
    public Class<? extends Partitioner> getPartitionerClass() {
        return SamtoolsReduce.SamtoolsPartitioner.class;
    }

    @Override
    public Class<? extends WritableComparator> getGrouperClass() {
        return SamtoolsReduce.SamtoolsGrouper.class;
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String cuff_binary_str = conf.get(cuff_binary_arg.getName());
        if(!new File(cuff_binary_str).exists())
            throw new IOException("Cannot find Cufflinks binary");

        cufflinks_cmd = String.format("%s /dev/stdin", cuff_binary_str);
    }

    @Override
    protected void reduce(SamtoolsMap.SamtoolsKey key, Iterable<SAMRecordWritable> values, Context context)
            throws IOException, InterruptedException {
        Process process = Runtime.getRuntime().exec(cufflinks_cmd);

        Thread connectout = new Thread(new ThreadedStreamConnector(process.getInputStream(),System.out));
        Thread connecterr = new Thread(new ThreadedStreamConnector(process.getErrorStream(),System.err));
        connectout.start();
        connecterr.start();

        boolean first = true;
        PrintWriter writer = new PrintWriter(process.getOutputStream());
        for(SAMRecordWritable read: values){
            if(first)
                writer.println(read.getTextHeader());
            writer.println(read);
        }
        writer.close();

        connectout.join();
        connecterr.join();
        process.waitFor();
        
        Configuration conf = context.getConfiguration();
        String outdir = conf.get("mapred.output.dir");
        FileSystem fs = FileSystem.get(conf);
        Path chrOut = new Path(outdir+"-"+key.getRef());
        fs.mkdirs(chrOut);
        
        String[] localFiles = new String[]{"transcripts.gtf","skipped.gtf",
                "isoforms.fpkm_tracking","genes.fpkm_tracking"};

        for(String fileStr : localFiles){
            fs.copyFromLocalFile(new Path("fileStr"),new Path(chrOut+"/"+fileStr));
        }

        fs.close();
    }
}
