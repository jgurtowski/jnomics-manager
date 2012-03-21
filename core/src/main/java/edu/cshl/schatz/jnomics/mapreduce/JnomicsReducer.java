package edu.cshl.schatz.jnomics.mapreduce;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.WritableComparator;

public abstract class JnomicsReducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
        extends Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {

    public JnomicsReducer(){}

    public abstract Class getOutputKeyClass();
    public abstract Class getOutputValueClass();
    public abstract JnomicsArgument[] getArgs();
    
    public Class<? extends Partitioner> getPartitionerClass(){return null;}
    public Class<? extends WritableComparator> getGrouperClass(){return null;}
}
