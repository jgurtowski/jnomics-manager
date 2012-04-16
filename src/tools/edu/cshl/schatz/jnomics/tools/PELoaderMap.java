package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsMapper;
import edu.cshl.schatz.jnomics.ob.writable.StringArrayWritable;
import org.apache.hadoop.io.IntWritable;


/**
 * User: james
 */
public class PELoaderMap<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
        extends JnomicsMapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>{

    @Override
    public Class getOutputKeyClass() {
        return IntWritable.class;
    }

    @Override
    public Class getOutputValueClass() {
        return StringArrayWritable.class;
    }

    @Override
    public JnomicsArgument[] getArgs() {
        return new JnomicsArgument[0];
    }
}
