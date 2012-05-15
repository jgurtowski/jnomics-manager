package edu.cshl.schatz.jnomics.kbase.thrift.server;

import edu.cshl.schatz.jnomics.kbase.thrift.api.Authentication;
import edu.cshl.schatz.jnomics.kbase.thrift.api.JnomicsCompute;
import edu.cshl.schatz.jnomics.kbase.thrift.api.JnomicsJobID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
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

    private Configuration getGenericConf(String inputPath, String outputPath){
        Configuration conf = new Configuration();
        conf.set("mapred.create.symlink","yes");
        conf.set("mapreduce.job.cache.archives.visibilities","true");
        conf.set("mapred.used.genericoptionsparser","true");
        conf.set("mapred.mapper.new-api","true");
        conf.set("mapred.input.dir",inputPath);
        conf.set("mapred.output.dir",outputPath);
        conf.set("fs.default.name",properties.getProperty("hdfs-default-name"));

        return conf;
    }

    private Configuration getAlignConf(String inputPath, String outputPath){
        Configuration conf = getGenericConf(inputPath,outputPath);
        conf.set("mapred.mapoutput.key.class","edu.cshl.schatz.jnomics.ob.SAMRecordWritable");
        conf.set("mapred.output.key.class","edu.cshl.schatz.jnomics.ob.SAMRecordWritable");
        conf.set("mapreduce.inputformat.class","org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat");
        conf.set("mapred.output.value.class","org.apache.hadoop.io.NullWritable");
        conf.set("mapreduce.outputformat.class","org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat");
        conf.set("mapred.mapoutput.value.class","org.apache.hadoop.io.NullWritable");
        conf.set("mapred.reduce.tasks","0");

        return conf;
    }


    @Override
    public JnomicsJobID alignBowtie(String inPath, String organism, String outPath, Authentication auth) throws TException {
        logger.info("Starting Bowtie2 process");
        final Configuration conf = getAlignConf(inPath,outPath);
        conf.set("mapreduce.map.class","edu.cshl.schatz.jnomics.tools.Bowtie2Map");
        String fsDefault = properties.getProperty("hdfs-default-name");
        conf.set("mapred.cache.archives",fsDefault+"user/"+auth.getUsername()+"/"+organism+"_bowtie.tar.gz#btarchive");
        conf.set("bowtie_binary","btarchive/bowtie2-align");
        conf.set("bowtie_idx", "btarchive/"+organism);
        conf.set("mapred.jar", "file:///Users/james/workspace/jnomics-code/jnomics-tools.jar");
        conf.set("mapred.job.name",auth.getUsername()+"-bowtie2-"+inPath);
        return launchJobAs(auth.getUsername(),conf);
    }


    @Override
    public JnomicsJobID alignBWA(String inPath, String organism, String outPath, Authentication auth) throws TException {
        return new JnomicsJobID("id");
    }

    @Override
    public JnomicsJobID snpSamtools(String inPath, String outPath, Authentication auth) throws TException {
        System.out.println("Samtools");
        return null;
    }

    public JnomicsJobID launchJobAs(String username,
                                    final Configuration conf){
        RunningJob runningJob = null;
        final String jobTracker = properties.getProperty("mapreduce-jobtracker-host");
        final int jobTrackerPort = Integer.parseInt(properties.getProperty("mapreduce-jobtracker-port"));

        try {
            UserGroupInformation.createRemoteUser(username).doAs(new PrivilegedExceptionAction<Integer>() {
                @Override
                public Integer run() throws Exception {
                    JobConf jConf = new JobConf(conf);
                    JobClient jobClient = new JobClient(new InetSocketAddress(jobTracker,jobTrackerPort),jConf);
                    //JobClient jobClient = new JobClient(conf);
                    jobClient.submitJob(jConf);
                    return new Integer(1);
                }
            });
        }catch (Exception e){
            logger.error("Failed to run Job");
            e.printStackTrace();
            return new JnomicsJobID(e.toString());
        }

        String jobid = runningJob.getID().toString();
        logger.info("submitted job: " + conf.get("mapred.job.name") + " " + jobid);
        return new JnomicsJobID(jobid);
    }
}
