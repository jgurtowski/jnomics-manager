package edu.cshl.schatz.jnomics.manager.client;

import edu.cshl.schatz.jnomics.manager.client.ann.Flag;
import edu.cshl.schatz.jnomics.manager.client.ann.FunctionDescription;
import edu.cshl.schatz.jnomics.manager.client.ann.KbaseScript;
import edu.cshl.schatz.jnomics.manager.client.compute.*;
import edu.cshl.schatz.jnomics.manager.client.fs.ListGenomes;

import java.util.List;
import java.util.Properties;

/**
 * User: james
 */

@FunctionDescription(description = "Compute functions that launch jobs on the Cluster.\n"+
        "There are also a number of utility functions that help with monitoring\n"
)
@KbaseScript(prefix = "compute",
        exportFields = {"bowtie","bwa","shock_batch_write","list_genomes","vcf_merge","list_jobs","job_status","samtools_snp"})
public class Compute implements ClientFunctionHandler {
 
    @Flag(shortForm = "bowtie", longForm = "bowtie", description = "Run Bowtie Aligner",group="Alignment")
    public boolean bowtie;

    @Flag(shortForm = "bwa", longForm = "bwa", description = "Run BWA Aligner",group="Alignment")
    public boolean bwa;
    
    @Flag(shortForm = "shock_batch_write", longForm = "--shockBatchWrite", description = "Copy Cluster files to Shock- Batch mode")
    public boolean shock_batch_write ;
    
    @Flag(shortForm ="list_genomes", longForm = "list_genomes", description = "List available genomes", group="Utility")
    public boolean list_genomes;

    @Flag(shortForm = "vcf_merge", longForm = "vcf_merge", description="Merge vcf files produced by variation pipeline", group="Utility")
    public boolean vcf_merge;

    @Flag(shortForm = "list_jobs", longForm = "list_jobs", description="List all of your jobs",group="Utility")
    public boolean list_jobs;

    @Flag(shortForm = "status", longForm ="job_status", description="List the status of a running job",group="Utility")
    public boolean job_status;

    @Flag(shortForm = "samtools_snp", longForm = "samtools_snp", description="Run Samtools Variation pipeline",group="Variation Detection")
    public boolean samtools_snp;


    @Override
    public void handle(List<String> remainingArgs,Properties properties) throws Exception {

        Class< ? extends ClientFunctionHandler> handlerClass = null;
        
        if(bowtie){
            handlerClass = Bowtie.class;
        }else if(bwa){
            handlerClass = BWA.class;
        }else if(shock_batch_write){
            handlerClass = ShockBatchWrite.class;
        }else if(list_genomes){
            handlerClass = ListGenomes.class;
        }else if(vcf_merge){
            handlerClass = VCFMerge.class;
        }else if(list_jobs){
            handlerClass = ListJobs.class;
        }else if(job_status){
            handlerClass = JobStatus.class;
        }else if(samtools_snp){
            handlerClass = SamtoolsSnp.class;
        }else{
            System.out.println(Utility.helpFromParameters(this.getClass()));
        }

        if(null != handlerClass){
            CreatedHandler createdHandler = Utility.handlerFromArgs(remainingArgs, handlerClass);
            createdHandler.getHandler().handle(createdHandler.getRemainingArgs(),properties);
        }
    }
}
