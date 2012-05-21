package edu.cshl.schatz.jnomics.manager.server;

import edu.cshl.schatz.jnomics.manager.api.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.thrift.TException;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
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
        conf.set("mapred.reduce.new-api","true");
        conf.set("mapred.input.dir",inputPath);
        conf.set("mapred.output.dir",outputPath);
        conf.set("fs.default.name", properties.getProperty("hdfs-default-name"));
        conf.set("mapred.jar", properties.getProperty("jnomics-jar-path"));

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
    public JnomicsThriftJobID alignBowtie(String inPath, String organism, String outPath, Authentication auth) throws TException, JnomicsThriftException {
        logger.info("Starting Bowtie2 process");
        Configuration conf = getAlignConf(inPath,outPath);
        conf.set("mapreduce.map.class","edu.cshl.schatz.jnomics.tools.Bowtie2Map");
        conf.set("mapred.cache.archives",properties.getProperty("hdfs-index-repo")+"/"+organism+"_bowtie.tar.gz#btarchive");
        conf.set("bowtie_binary","btarchive/bowtie2-align");
        conf.set("bowtie_idx", "btarchive/"+organism);
        conf.set("mapred.job.name",auth.getUsername()+"-bowtie2-"+inPath);
        return launchJobAs(auth.getUsername(),conf);
    }


    @Override
    public JnomicsThriftJobID alignBWA(String inPath, String organism, String outPath, Authentication auth) throws TException, JnomicsThriftException {
        logger.info("Starting Bwa process");
        Configuration conf = getAlignConf(inPath,outPath);
        conf.set("mapreduce.map.class","edu.cshl.schatz.jnomics.tools.BWAMap");
        conf.set("mapred.cache.archives",properties.getProperty("hdfs-index-repo")+"/"+organism+"_bwa.tar.gz#bwaarchive");
        conf.set("bwa_binary","bwaarchive/bwa");
        conf.set("bwa_index", "bwaarchive/"+organism);
        conf.set("mapred.job.name",auth.getUsername()+"-bwa-"+inPath);
        return launchJobAs(auth.getUsername(),conf);
    }

    @Override
    public JnomicsThriftJobID snpSamtools(String inPath, String organism, String outPath, Authentication auth) throws TException, JnomicsThriftException {
        Configuration conf = getGenericConf(inPath,outPath);
        conf.set("mapred.mapoutput.key.class","edu.cshl.schatz.jnomics.tools.SamtoolsMap$SamtoolsKey");
        conf.set("mapred.mapoutput.value.class","edu.cshl.schatz.jnomics.ob.SAMRecordWritable");
        conf.set("mapred.output.key.class","org.apache.hadoop.io.Text");
        conf.set("mapred.output.value.class","org.apache.hadoop.io.NullWritable");
        conf.set("mapreduce.inputformat.class","org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat");
        conf.set("mapreduce.outputformat.class","org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat");
        conf.set("mapreduce.map.class","edu.cshl.schatz.jnomics.tools.SamtoolsMap");
        conf.set("mapreduce.reduce.class","edu.cshl.schatz.jnomics.tools.SamtoolsReduce");
        conf.set("mapred.output.value.groupfn.class","edu.cshl.schatz.jnomics.tools.SamtoolsReduce$SamtoolsGrouper");
        conf.set("mapreduce.partitioner.class","edu.cshl.schatz.jnomics.tools.SamtoolsReduce$SamtoolsPartitioner");
        conf.set("mapred.cache.archives",properties.getProperty("hdfs-index-repo")+"/"+organism+"_samtools.tar.gz#starchive");
        conf.set("samtools_bin","starchive/samtools");
        conf.set("bcftools_bin","starchive/bcftools");
        conf.set("reference_fa","starchive/"+organism+".fa");
        conf.set("genome_binsize","1000000");
        conf.setInt("mapred.reduce.tasks",96);
        conf.set("mapred.job.name",auth.getUsername()+"-snp-"+inPath);
        return launchJobAs(auth.getUsername(), conf);
    }

    @Override
    public JnomicsThriftJobStatus getJobStatus(final JnomicsThriftJobID jobID, final Authentication auth) throws TException, JnomicsThriftException {
        JnomicsThriftJobStatus runningJob = new JobClientRunner<JnomicsThriftJobStatus>(auth.getUsername(),new Configuration(),properties){
            @Override
            public JnomicsThriftJobStatus jobClientTask() throws Exception {

                RunningJob job = getJobClient().getJob(JobID.forName(jobID.getJob_id()));
                return new JnomicsThriftJobStatus(job.getID().toString(),
                        auth.getUsername(),
                        null,
                        job.isComplete(),
                        job.getJobState(),
                        0,
                        null,
                        job.mapProgress(),
                        job.reduceProgress());
            }
        }.run();
        return runningJob;
    }

    @Override
    public List<JnomicsThriftJobStatus> getAllJobs(Authentication auth) throws JnomicsThriftException, TException {
        JobStatus[] statuses = new JobClientRunner<JobStatus[]>(auth.getUsername(),new Configuration(),properties){
            @Override
            public JobStatus[] jobClientTask() throws Exception {
                logger.info("getting jobs");
                return getJobClient().getAllJobs();
            }
        }.run();
        logger.info("got jobs");
        List<JnomicsThriftJobStatus> newStats = new ArrayList<JnomicsThriftJobStatus>();
        for(JobStatus stat: statuses){
            if(0 == auth.getUsername().compareTo(stat.getUsername()))
                newStats.add(new JnomicsThriftJobStatus(stat.getJobID().toString(),
                        stat.getUsername(),
                        stat.getFailureInfo(),
                        stat.isJobComplete(),
                        stat.getRunState(),
                        stat.getStartTime(),
                        stat.getJobPriority().toString(),
                        stat.mapProgress(),
                        stat.reduceProgress()));
        }
        return newStats;
    }

    public JnomicsThriftJobID launchJobAs(String username,
                                          final Configuration conf) throws JnomicsThriftException {
        RunningJob runningJob = new JobClientRunner<RunningJob>(username,conf,properties){
            @Override
            public RunningJob jobClientTask() throws Exception {
                return getJobClient().submitJob(getJobConf());
            }
        }.run();
        String jobid = runningJob.getID().toString();
        logger.info("submitted job: " + conf.get("mapred.job.name") + " " + jobid);
        return new JnomicsThriftJobID(jobid);
    }
}
