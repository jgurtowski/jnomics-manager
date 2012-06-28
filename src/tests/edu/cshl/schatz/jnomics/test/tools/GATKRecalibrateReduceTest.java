package edu.cshl.schatz.jnomics.test.tools;

import org.apache.hadoop.conf.Configuration;

/**
 * User: james
 */
public class GATKRecalibrateReduceTest {

    public static void main(String []args){

        Configuration conf = new Configuration();
        conf.set("flag","2");

        long flag = Long.parseLong(conf.get("flag"),16);
        System.out.println(flag);

        System.out.println(flag & 153);

    }
}
