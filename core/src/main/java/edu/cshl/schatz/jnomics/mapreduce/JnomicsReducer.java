package edu.cshl.schatz.jnomics.mapreduce;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import org.apache.hadoop.mapreduce.Reducer;


public abstract class JnomicsReducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
        extends Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {

    public JnomicsReducer(){}


    public abstract Class getOutputKeyClass();
    public abstract Class getOutputValueClass();
    public abstract JnomicsArgument[] getArgs();
    
    public Class getPartitionerClass(){return null;}
    public Class getGrouperClass(){return null;}
}
