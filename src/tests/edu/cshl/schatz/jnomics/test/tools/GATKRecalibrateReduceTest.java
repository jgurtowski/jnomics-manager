package edu.cshl.schatz.jnomics.test.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer;
import edu.cshl.schatz.jnomics.tools.GATKRecalibrateReduce;

/**
 * User: james
 */
public class GATKRecalibrateReduceTest {

    public static void main(String []args) throws IllegalAccessException, InstantiationException {
        JnomicsReducer reducer = GATKRecalibrateReduce.class.newInstance();
        for(JnomicsArgument arg: reducer.getArgs()){
            System.out.println(arg.getName());
        }
    }
}
