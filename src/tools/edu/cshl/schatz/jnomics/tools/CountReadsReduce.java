package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * User: james
 */
public class CountReadsReduce extends JnomicsReducer<Text,IntWritable, Text, IntWritable> {

    private final IntWritable count_writable = new IntWritable();
    private int count;

    @Override
    public Class getOutputKeyClass() {
        return Text.class;
    }

    @Override
    public Class getOutputValueClass() {
        return IntWritable.class;
    }

    @Override
    public JnomicsArgument[] getArgs() {
        return new JnomicsArgument[0];
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        count = 0;
        for(IntWritable c: values){
            count += c.get();
        }
        count_writable.set(count);
        context.write(key,count_writable);
    }
}
