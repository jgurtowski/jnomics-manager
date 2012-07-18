package edu.cshl.schatz.jnomics.test.tools;

import edu.cshl.schatz.jnomics.tools.BWAMap;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * User: james
 */
public class ConfigurationTest extends TestCase {

    public void testConf(){
        Configuration conf = new Configuration();
        System.out.println(conf);
        conf.addResource(new Path("/home/james/sources/hadoop/conf/mapred-site.xml"));
        System.out.println(conf);
        System.out.println(conf.get("mapred.output.compression.type"));
        System.out.println(BWAMap.class.getName());
    }
}
