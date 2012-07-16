package edu.cshl.schatz.jnomics.manager.server;

import edu.cshl.schatz.jnomics.manager.api.*;
import edu.cshl.schatz.jnomics.tools.CovariateMerge;
import edu.cshl.schatz.jnomics.tools.VCFMerge;
import edu.cshl.schatz.jnomics.util.TextUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

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
        //if you don't give Path's it will not load the files
        conf.addResource(new Path(properties.getProperty("core-site-xml")));
        conf.addResource(new Path(properties.getProperty("mapred-site-xml")));
        conf.addResource(new Path(properties.getProperty("hdfs-site-xml")));

        conf.set("mapred.used.genericoptionsparser","true");
        conf.set("mapred.mapper.new-api","true");
        conf.set("mapred.reducer.new-api","true");
        conf.set("mapred.input.dir",inputPath);
        conf.set("mapred.output.dir",outputPath);
        conf.set("fs.default.name", properties.getProperty("hdfs-default-name"));
        conf.set("mapred.jar", properties.getProperty("jnomics-jar-path"));
        return conf;
    }

    private Configuration getAlignConf(String inputPath, String outputPath){
        Configuration conf = getGenericConf(inputPath,outputPath);
        conf.set("mapred.create.symlink","yes");
        conf.set("mapreduce.job.cache.archives.visibilities","true");
        conf.set("mapred.mapoutput.key.class","edu.cshl.schatz.jnomics.ob.AlignmentCollectionWritable");
        conf.set("mapred.output.key.class","edu.cshl.schatz.jnomics.ob.AlignmentCollectionWritable");
        conf.set("mapreduce.inputformat.class","org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat");
        conf.set("mapred.output.value.class","org.apache.hadoop.io.NullWritable");
        conf.set("mapreduce.outputformat.class","org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat");
        conf.set("mapred.mapoutput.value.class","org.apache.hadoop.io.NullWritable");
        conf.set("mapred.reduce.tasks","0");
        return conf;
    }

    @Override
    public JnomicsThriftJobID alignBowtie(String inPath, String organism, String outPath, String opts, Authentication auth)
            throws TException, JnomicsThriftException {
        logger.info("Starting Bowtie2 process");
        Configuration conf = getAlignConf(inPath,outPath);
        conf.set("mapreduce.map.class","edu.cshl.schatz.jnomics.tools.Bowtie2Map");
        conf.set("mapred.cache.archives",properties.getProperty("hdfs-index-repo")+"/"+organism+"_bowtie.tar.gz#btarchive"
                +","+ properties.getProperty("hdfs-index-repo")+"/bowtie.tar.gz#bowtie");
        conf.set("bowtie_binary","bowtie/bowtie2-align");
        conf.set("bowtie_index", "btarchive/"+organism+".fa");
        conf.set("bowtie_opts",opts);
        conf.set("mapred.job.name",auth.getUsername()+"-bowtie2-"+inPath);
        return launchJobAs(auth.getUsername(),conf);
    }

    @Override
    public JnomicsThriftJobID alignBWA(String inPath, String organism, String outPath, 
                                       String alignOpts, String sampeOpts, Authentication auth)
            throws TException, JnomicsThriftException {
        logger.info("Starting Bwa process");
        Configuration conf = getAlignConf(inPath,outPath);
        conf.set("mapreduce.map.class","edu.cshl.schatz.jnomics.tools.BWAMap");
        conf.set("mapred.cache.archives",properties.getProperty("hdfs-index-repo")+"/"+organism+"_bwa.tar.gz#bwaarchive"
                +","+properties.getProperty("hdfs-index-repo")+"/bwa.tar.gz#bwa");
        conf.set("bwa_binary","bwa/bwa");
        conf.set("bwa_index", "bwaarchive/"+organism+".fa");
        conf.set("bwa_align_opts",alignOpts);
        conf.set("bwa_sampe_opts",sampeOpts);
        conf.set("mapred.job.name",auth.getUsername()+"-bwa-"+inPath);
        return launchJobAs(auth.getUsername(),conf);
    }

    @Override
    public JnomicsThriftJobID snpSamtools(String inPath, String organism, String outPath, Authentication auth) throws TException, JnomicsThriftException {
        Configuration conf = getGenericConf(inPath,outPath);
        conf.set("mapred.create.symlink","yes");
        conf.set("mapreduce.job.cache.archives.visibilities","true");
        conf.set("mapred.mapoutput.key.class","edu.cshl.schatz.jnomics.tools.SamtoolsMap$SamtoolsKey");
        conf.set("mapred.mapoutput.value.class","edu.cshl.schatz.jnomics.ob.SAMRecordWritable");
        conf.set("mapred.output.key.class","org.apache.hadoop.io.Text");
        conf.set("mapred.output.value.class","org.apache.hadoop.io.NullWritable");
        conf.set("mapreduce.inputformat.class","org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat");
        conf.set("mapreduce.outputformat.class","org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat");
        conf.set("mapreduce.map.class","edu.cshl.schatz.jnomics.tools.SamtoolsMap");
        conf.set("mapreduce.reduce.class","edu.cshl.schatz.jnomics.tools.SamtoolsReduce");
        conf.set("mapred.output.value.groupfn.class","edu.cshl.schatz.jnomics.tools.SamtoolsReduce$SamtoolsGrouper");
        conf.set("mapred.reduce.tasks.speculative.execution", "false");
        conf.set("mapreduce.partitioner.class", "edu.cshl.schatz.jnomics.tools.SamtoolsReduce$SamtoolsPartitioner");
        conf.set("mapred.cache.archives",properties.getProperty("hdfs-index-repo")+"/"+organism+"_samtools.tar.gz#starchive"
        +","+properties.getProperty("hdfs-index-repo")+"/samtools.tar.gz#samtools"+","+
        properties.getProperty("hdfs-index-repo")+"/bcftools.tar.gz#bcftools");
        conf.set("samtools_binary","samtools/samtools");
        conf.set("bcftools_binary","bcftools/bcftools");
        conf.set("reference_fa","starchive/"+organism+".fa");
        conf.set("genome_binsize","1000000");
        conf.setInt("mapred.reduce.tasks",1024);
        conf.set("mapred.job.name",auth.getUsername()+"-snp-"+inPath);
        return launchJobAs(auth.getUsername(), conf);
    }

    @Override
    public JnomicsThriftJobStatus getJobStatus(final JnomicsThriftJobID jobID, final Authentication auth)
            throws TException, JnomicsThriftException {
        JnomicsThriftJobStatus runningJob = new JobClientRunner<JnomicsThriftJobStatus>(auth.getUsername(),
                new Configuration(),properties){
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


    /**
     * Writes a manifest file in a directory called manifests in home directory
     *
     * @param filename filename to use as prefix for manifest file
     * @param data data to write to manifest file, each string in the array is aline
     * @param username username to perform fs operations as
     * @return Path of the created manfiest file
     * @throws JnomicsThriftException
     */
    private Path writeManifest(String filename, String []data, String username) throws JnomicsThriftException{
        //write manifest file and run job
        FileSystem fs = null;
        try {
            fs = FileSystem.get(new URI(properties.getProperty("hdfs-default-name")),
                    new Configuration(),username);
            if(!fs.exists(new Path("manifests"))){
                fs.mkdirs(new Path("manifests"));
            }
        }catch (Exception e) {
            try{
                fs.close();
            }catch (Exception t){
                throw new JnomicsThriftException(t.toString());
            }
            new JnomicsThriftException(e.toString());
        }

        Path manifest,f1;
        FSDataOutputStream outStream = null;
        try{
            f1 = new Path(filename);
            manifest = new Path("manifests/"+f1.getName()+"-"+UUID.randomUUID().toString()+".manifest");
            outStream = fs.create(manifest);
            for(String line: data){
                outStream.write((data + "\n").getBytes());
            }
        }catch(Exception e){
            throw new JnomicsThriftException(e.toString());
        }finally{
            try {
                outStream.close();
            } catch (IOException e) {
                throw new JnomicsThriftException();
            }finally{
                try{
                    fs.close();
                }catch(Exception b){
                    throw new JnomicsThriftException(b.toString());
                }
            }
        }
        return manifest;
    }


    @Override
    public JnomicsThriftJobID pairReads(String file1, String file2, String outFile, Authentication auth)
            throws JnomicsThriftException, TException {

        String data = TextUtil.join("\t",new String[]{file1,file2,outFile});
        Path manifest = writeManifest(new File(file1).getName(), new String[]{data}, auth.getUsername());

        Path manifestlog = new Path(manifest +".log");
        Configuration conf = getGenericConf(manifest.toString(),manifestlog.toString());
        conf.set("mapred.job.name",new File(file1).getName()+"-pe-conversion");
        conf.set("mapred.mapoutput.key.class","org.apache.hadoop.io.IntWritable");
        conf.set("mapred.mapoutput.value.class","edu.cshl.schatz.jnomics.ob.writable.PEMetaInfo");
        conf.set("mapred.output.key.class","org.apache.hadoop.io.Text");
        conf.set("mapred.output.value.class","org.apache.hadoop.io.NullWritable");
        conf.set("mapreduce.inputformat.class","org.apache.hadoop.mapreduce.lib.input.TextInputFormat");
        conf.set("mapreduce.outputformat.class","org.apache.hadoop.mapreduce.lib.output.TextOutputFormat");
        conf.set("mapreduce.map.class","edu.cshl.schatz.jnomics.tools.PELoaderMap");
        conf.set("mapreduce.reduce.class","edu.cshl.schatz.jnomics.tools.PELoaderReduce");
        conf.setInt("mapred.reduce.tasks",1);
        String username = auth.getUsername();
        JnomicsThriftJobID id = launchJobAs(username,conf);
        logger.info("submitted job: " + conf.get("mapred.job.name") + " " + id);
        return new JnomicsThriftJobID(id);
    }

    @Override
    public JnomicsThriftJobID singleReads(String file, String outFile, Authentication auth)
            throws JnomicsThriftException, TException {

        String fileBase = new File(file).getName();
        String username = auth.getUsername();

        String data = TextUtil.join("\t",new String[]{file,outFile});
        Path manifest = writeManifest(fileBase,new String[]{data},username);
        
        Path manifestlog = new Path(manifest + ".log");

        Configuration conf = getGenericConf(manifest.toString(),manifestlog.toString());
        conf.set("mapred.job.name",fileBase+"-pe-conversion");
        conf.set("mapred.mapoutput.key.class","org.apache.hadoop.io.IntWritable");
        conf.set("mapred.mapoutput.value.class","edu.cshl.schatz.jnomics.ob.writable.SEMetaInfo");
        conf.set("mapred.output.key.class","org.apache.hadoop.io.Text");
        conf.set("mapred.output.value.class","org.apache.hadoop.io.NullWritable");
        conf.set("mapreduce.inputformat.class","org.apache.hadoop.mapreduce.lib.input.TextInputFormat");
        conf.set("mapreduce.outputformat.class","org.apache.hadoop.mapreduce.lib.output.TextOutputFormat");
        conf.set("mapreduce.map.class","edu.cshl.schatz.jnomics.tools.SELoaderMap");
        conf.set("mapreduce.reduce.class","edu.cshl.schatz.jnomics.tools.SELoaderReduce");
        conf.setInt("mapred.reduce.tasks",1);
        JnomicsThriftJobID id = launchJobAs(username,conf);
        logger.info("submitted job: " + conf.get("mapred.job.name") + " " + id);
        return new JnomicsThriftJobID(id);        
        
    }

    public JnomicsThriftJobID launchJobAs(String username, final Configuration conf)
            throws JnomicsThriftException {
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

    @Override
    public boolean mergeVCF(String inDir, String inAlignments, String outVCF, Authentication auth) throws JnomicsThriftException, TException {
        final Configuration conf = getGenericConf(inDir,outVCF);
        logger.info("Merging VCF: " + inDir + ":" + inAlignments + ":" + outVCF);
        final Path in = new Path(inDir);
        final Path alignments = new Path(inAlignments);
        final Path out = new Path(outVCF);
        boolean status  = false;
        try {
            status = UserGroupInformation.createRemoteUser(auth.getUsername()).doAs(new PrivilegedExceptionAction<Boolean>() {
                @Override
                public Boolean run() throws Exception {
                    VCFMerge.merge(in,alignments,out,conf);
                    return true;
                }
            });
        }catch (Exception e) {
            logger.info("Failed to merge: " + e.toString());
            throw new JnomicsThriftException(e.toString());
        }
        return status;
    }

    @Override
    public boolean mergeCovariate(String inDir, String outCov, Authentication auth) throws JnomicsThriftException, TException {
        final Configuration conf = getGenericConf(inDir,outCov);
        final Path in = new Path(inDir);
        final Path out = new Path(outCov);

        boolean status = false;
        try{
            status = UserGroupInformation.createRemoteUser(auth.getUsername()).doAs(new PrivilegedExceptionAction<Boolean>() {
                @Override
                public Boolean run() throws Exception {
                    CovariateMerge.merge(in, out, conf);
                    return true;
                }
            });
        }catch(Exception e){
            logger.info("Failed to merge:" + e.toString());
            throw new JnomicsThriftException(e.toString());
        }
        return true;
    }

    private Configuration getGATKConf(String inPath, String outPath, String organism){
        Configuration conf = getGenericConf(inPath,outPath);
        conf.set("mapred.create.symlink","yes");
        conf.set("mapreduce.job.cache.archives.visibilities","true");
        conf.set("mapred.mapoutput.key.class","edu.cshl.schatz.jnomics.tools.SamtoolsMap$SamtoolsKey");
        conf.set("mapred.mapoutput.value.class","edu.cshl.schatz.jnomics.ob.SAMRecordWritable");
        conf.set("mapreduce.inputformat.class","org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat");
        conf.set("mapreduce.outputformat.class","org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat");
        conf.set("mapreduce.map.class","edu.cshl.schatz.jnomics.tools.SamtoolsMap");
        conf.set("mapred.output.value.groupfn.class","edu.cshl.schatz.jnomics.tools.SamtoolsReduce$SamtoolsGrouper");
        conf.set("mapreduce.partitioner.class","edu.cshl.schatz.jnomics.tools.SamtoolsReduce$SamtoolsPartitioner");
        conf.set("mapred.cache.archives",properties.getProperty("hdfs-index-repo")+"/"+organism+"_gatk.tar.gz#gatk");
        conf.set("samtools_binary","gatk/samtools");
        conf.set("reference_fa","gatk/"+organism+".fa");
        conf.set("gatk_jar", "gatk/GenomeAnalysisTK.jar");
        conf.set("genome_binsize","1000000");
        conf.setInt("mapred.reduce.tasks",1024);
        return conf;
    }
    
    @Override
    public JnomicsThriftJobID gatkRealign(String inPath, String organism, String outPath, Authentication auth)
            throws JnomicsThriftException, TException {
        Configuration conf = getGATKConf(inPath,outPath,organism);
        conf.set("mapred.output.key.class","edu.cshl.schatz.jnomics.ob.SAMRecordWritable");
        conf.set("mapred.output.value.class","org.apache.hadoop.io.NullWritable");
        conf.set("mapreduce.reduce.class","edu.cshl.schatz.jnomics.tools.GATKRealignReduce");
        conf.set("mapred.job.name",auth.getUsername()+"-gatk-realign-"+inPath);
        return launchJobAs(auth.getUsername(), conf);
    }

    @Override
    public JnomicsThriftJobID gatkCallVariants(String inPath, String organism, String outPath, Authentication auth)
            throws JnomicsThriftException, TException {
        Configuration conf = getGATKConf(inPath,outPath,organism);
        conf.set("mapred.output.key.class","org.apache.hadoop.io.NullWritable");
        conf.set("mapred.output.value.class","org.apache.hadoop.io.NullWritable");
        conf.set("mapreduce.reduce.class","edu.cshl.schatz.jnomics.tools.GATKCallVarReduce");
        conf.set("mapred.job.name",auth.getUsername()+"-gatk-call-variants");
        conf.set("mapred.reduce.tasks.speculative.execution", "false");
        return launchJobAs(auth.getUsername(), conf);
    }

    @Override
    public JnomicsThriftJobID gatkCountCovariates(String inPath, String organism, String vcfMask, String outPath, Authentication auth)
            throws JnomicsThriftException, TException {
        Configuration conf = getGATKConf(inPath,outPath,organism);
        conf.set("mapred.output.key.class","org.apache.hadoop.io.NullWritable");
        conf.set("mapred.output.value.class","org.apache.hadoop.io.NullWritable");
        conf.set("mapreduce.reduce.class","edu.cshl.schatz.jnomics.tools.GATKCountCovariatesReduce");
        Path vcfMaskPath = new Path(vcfMask);
        conf.set("mared.cache.files",vcfMaskPath.toString()+"#"+vcfMaskPath.getName());
        conf.set("tmpfiles",vcfMaskPath.toString()+"#"+vcfMaskPath.getName());
        conf.set("vcf_mask",vcfMaskPath.getName());
        conf.set("mapred.job.name",auth.getUsername()+"-gatk-count-covariates");
        return launchJobAs(auth.getUsername(),conf);
    }

    @Override
    public JnomicsThriftJobID gatkRecalibrate(String inPath, String organism, String recalFile, String outPath, Authentication auth)
            throws JnomicsThriftException, TException {
        Configuration conf = getGATKConf(inPath,outPath,organism);
        conf.set("mapred.output.key.class","edu.cshl.schatz.jnomics.ob.SAMRecordWritable");
        conf.set("mapred.output.value.class","org.apache.hadoop.io.NullWritable");
        conf.set("mapreduce.reduce.class","edu.cshl.schatz.jnomics.tools.GATKRecalibrateReduce");
        Path recalFilePath = new Path(recalFile);
        conf.set("mapred.cache.files", recalFilePath.toString()+"#"+recalFilePath.getName());
        conf.set("tmpfiles", recalFilePath.toString()+"#"+recalFilePath.getName());
        conf.set("recal_file",recalFilePath.getName());
        conf.set("mapred.job.name",auth.getUsername()+"-gatk-recalibrate");
        return launchJobAs(auth.getUsername(),conf);
    }
}
