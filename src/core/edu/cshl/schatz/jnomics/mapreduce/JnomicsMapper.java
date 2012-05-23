package edu.cshl.schatz.jnomics.mapreduce;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;


public abstract class JnomicsMapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
        extends Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {

    public JnomicsMapper(){}

    public abstract Class getOutputKeyClass();
    public abstract Class getOutputValueClass();
    public abstract JnomicsArgument[] getArgs();
    
    public Class<? extends Reducer> getCombinerClass(){return null;}
    
    public Class<? extends InputFormat> getInputFormat(){return SequenceFileInputFormat.class;}

}                

