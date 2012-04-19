package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsMapper;
import edu.cshl.schatz.jnomics.ob.ReadCollectionWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * User: james
 */
public class CountReadsMap extends JnomicsMapper<ReadCollectionWritable,NullWritable,Text,IntWritable> {

    private final Text name = new Text("Read Count");
    private final IntWritable count = new IntWritable(1);
    
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
    public Class<? extends Reducer> getCombinerClass() {
        return CountReadsReduce.class;
    }

    @Override
    protected void map(ReadCollectionWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
        context.write(name,count);
    }
}
