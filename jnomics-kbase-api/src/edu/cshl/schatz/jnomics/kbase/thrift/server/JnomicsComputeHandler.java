package edu.cshl.schatz.jnomics.kbase.thrift.server;

import edu.cshl.schatz.jnomics.kbase.thrift.api.Authentication;
import edu.cshl.schatz.jnomics.kbase.thrift.api.JnomicsCompute;
import edu.cshl.schatz.jnomics.kbase.thrift.api.JnomicsJobID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.thrift.TException;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Properties;

/**
  * User: james
 */
public class JnomicsComputeHandler implements JnomicsCompute.Iface{

    private final org.slf4j.Logger logger = LoggerFactory.getLogger(JnomicsComputeHandler.class);

    private Properties properties;

    public JnomicsComputeHandler(Properties systemProperties){
        properties = systemProperties;
    }

    private Configuration getGenericConf(){
        Configuration conf = new Configuration();
        conf.set("mapred.create.symlink","yes");
        conf.set("mapreduce.job.cache.archives.visibilities","true");
        conf.set("mapred.used.genericoptionsparser","true");
        conf.set("mapred.mapper.new-api","true");

        return conf;
    }

    
    @Override
    public JnomicsJobID alignBowtie(String inPath, String organism, String outPath, Authentication auth) throws TException {
        logger.info("Starting Bowtie2 process");
        Configuration conf = getGenericConf();
        conf.set("user.name",auth.getUsername());
        String fsDefault = properties.getProperty("hdfs-default-name");
        String jobtracker =  properties.getProperty("mapreduce-jobtracker-host");
        int jobtrackerPort = Integer.parseInt(properties.getProperty("mapreduce-jobtracker-port"));
        conf.set("fs.default.name", fsDefault);
        conf.set("mapred.mapoutput.key.class","edu.cshl.schatz.jnomics.ob.SAMRecordWritable");
        conf.set("mapred.output.key.class","edu.cshl.schatz.jnomics.ob.SAMRecordWritable");
        conf.set("mapreduce.inputformat.class","org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat");
        conf.set("mapred.output.value.class","org.apache.hadoop.io.NullWritable");
        conf.set("mapreduce.outputformat.class","org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat");
        conf.set("mapred.mapoutput.value.class","org.apache.hadoop.io.NullWritable");
        conf.set("mapreduce.map.class","edu.cshl.schatz.jnomics.tools.Bowtie2Map");
        conf.set("mapred.input.dir",inPath);
        conf.set("mapred.output.dir",outPath);
        conf.set("mapred.cache.archives",fsDefault+"user/"+auth.getUsername()+"/"+organism+"_bowtie.tar.gz#btarchive");
        conf.set("bowtie_binary","btarchive/bowtie2-align");
        conf.set("bowtie_idx", "btarchive/"+organism);
        conf.set("mapred.jar", "file:///home/james/workspace/jnomics-code/jnomics-tools.jar");
        conf.set("mapred.job.name",auth.getUsername()+"-bowtie2-"+inPath);
        conf.set("mapred.reduce.tasks","0");

        RunningJob runningJob = null;
        try {
            final JobClient jobClient = new JobClient(new InetSocketAddress(jobtracker,jobtrackerPort),conf);
            final JobConf jConf = new JobConf(conf);
            runningJob = jobClient.submitJob(jConf);
            /*UserGroupInformation ugi = UserGroupInformation.createProxyUser("james", UserGroupInformation.getCurrentUser());

              @Override
              public Object run() {
                  try {
                      jobClient.submitJob(jConf);
                  } catch (IOException e) {
                  }
                  return null;
              }
          });  */
        } catch (Exception e){
            logger.error("Error: " + e.toString());
            e.printStackTrace();
            return new JnomicsJobID(e.toString());
        }

        String jobid = runningJob.getID().toString();
        logger.info("submitted Bowtie2 job: " + jobid);
        return new JnomicsJobID(jobid);
    }

    @Override
    public JnomicsJobID alignBWA(String inPath, String organism, String outPath, Authentication auth) throws TException {

        logger.info("Starting BWA process");
        Configuration conf = getGenericConf();
        conf.set("user.name",auth.getUsername());
        String fsDefault = properties.getProperty("hdfs-default-name");
        String jobtracker =  properties.getProperty("mapreduce-jobtracker-host");
        int jobtrackerPort = Integer.parseInt(properties.getProperty("mapreduce-jobtracker-port"));
        conf.set("fs.default.name", fsDefault);
        conf.set("mapred.mapoutput.key.class","edu.cshl.schatz.jnomics.ob.SAMRecordWritable");
        conf.set("mapred.output.key.class","edu.cshl.schatz.jnomics.ob.SAMRecordWritable");
        conf.set("mapreduce.inputformat.class","org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat");
        conf.set("mapred.output.value.class","org.apache.hadoop.io.NullWritable");
        conf.set("mapreduce.outputformat.class","org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat");
        conf.set("mapred.mapoutput.value.class","org.apache.hadoop.io.NullWritable");
        conf.set("mapreduce.map.class","edu.cshl.schatz.jnomics.tools.BWAMap");
        conf.set("mapred.input.dir",inPath);
        conf.set("mapred.output.dir",outPath);
        conf.set("mapred.cache.archives",fsDefault+"user/"+auth.getUsername()+"/"+organism+"_bwa.tar.gz#btarchive");
        conf.set("bowtie_binary","btarchive/bowtie2-align");
        conf.set("bowtie_idx", "btarchive/"+organism);
        conf.set("mapred.jar", "file:///home/james/workspace/jnomics-code/jnomics-tools.jar");
        conf.set("mapred.job.name",auth.getUsername()+"-bwa-"+inPath);
        conf.set("mapred.reduce.tasks","0");

        RunningJob runningJob = null;
        try {
            final JobClient jobClient = new JobClient(new InetSocketAddress(jobtracker,jobtrackerPort),conf);
            final JobConf jConf = new JobConf(conf);
            runningJob = jobClient.submitJob(jConf);
        } catch (Exception e){
            logger.error("Error: " + e.toString());
            e.printStackTrace();
            return new JnomicsJobID(e.toString());
        }

        String jobid = runningJob.getID().toString();
        logger.info("submitted Bowtie2 job: " + jobid);
        return new JnomicsJobID(jobid);
    }

    @Override
    public JnomicsJobID snpSamtools(String inPath, String outPath, Authentication auth) throws TException {
        System.out.println("Samtools");
        return null;
    }

}
