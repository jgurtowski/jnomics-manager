package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsMapper;
import edu.cshl.schatz.jnomics.ob.writable.PEMetaInfo;
import edu.cshl.schatz.jnomics.ob.writable.SEMetaInfo;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.util.Random;


/**
 * User: james
 */
public class SELoaderMap<KEY> extends JnomicsMapper<KEY,Text,IntWritable,SEMetaInfo>{

    private final SEMetaInfo info = new SEMetaInfo();
    private final IntWritable outKey = new IntWritable();
    private final Random random = new Random();
    private int reduceTasks;

    @Override
    public Class getOutputKeyClass() {
        return IntWritable.class;
    }

    @Override
    public Class getOutputValueClass() {
        return SEMetaInfo.class;
    }

    @Override
    public JnomicsArgument[] getArgs() {
        return new JnomicsArgument[0];
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        reduceTasks = context.getConfiguration().getInt("mapred.reduce.tasks",1);
    }

    @Override
    public Class<? extends InputFormat> getInputFormat() {
        return TextInputFormat.class;
    }

    @Override
    protected void map(KEY key, Text line, Context context) throws IOException, InterruptedException {
        String []args = line.toString().split("\t");

        if(args.length != 2)
            return;// Bad line

        info.setFile(args[0]);
        info.setDestination(args[1]+".se");
        outKey.set(random.nextInt(reduceTasks));
        context.write(outKey,info);
    }
}
