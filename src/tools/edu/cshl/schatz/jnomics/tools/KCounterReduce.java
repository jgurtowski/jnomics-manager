package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer;
import edu.cshl.schatz.jnomics.ob.FixedKmerWritable;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;

public class KCounterReduce extends JnomicsReducer <FixedKmerWritable, IntWritable, FixedKmerWritable, IntWritable> {

    private int totalCount;

    private final IntWritable totalCountWritable = new IntWritable();
    
    @Override
    public Class getOutputKeyClass() {
        return FixedKmerWritable.class;
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
    protected void reduce(FixedKmerWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        totalCount = 0;
        for(IntWritable count: values){
            totalCount += count.get();
        }
        totalCountWritable.set(totalCount);
        context.write(key,totalCountWritable);
    }
}
