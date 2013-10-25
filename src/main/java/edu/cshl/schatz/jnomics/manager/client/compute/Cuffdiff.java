package edu.cshl.schatz.jnomics.manager.client.compute;

import edu.cshl.schatz.jnomics.manager.api.JnomicsThriftFileStatus;
import edu.cshl.schatz.jnomics.manager.api.JnomicsThriftJobID;
import edu.cshl.schatz.jnomics.manager.client.Utility;
import edu.cshl.schatz.jnomics.manager.client.ann.Flag;
import edu.cshl.schatz.jnomics.manager.client.ann.FunctionDescription;
import edu.cshl.schatz.jnomics.manager.client.ann.Parameter;
import edu.cshl.schatz.jnomics.manager.common.KBaseIDTranslator;
import edu.cshl.schatz.jnomics.manager.client.fs.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * User: Sri
 */


@FunctionDescription(description = "Cuffdiff\n"+
        "identify differentially Expressed genes and transcripts .\n"+
        "Organism can be specified with the -ref flag. Input and \n"+
        "Output must reside on the Cluster's filesystem. \n"+
        "Optional additonal arguments may be supplied to \n"+
        "cuffdiff. These options are passed as a string to cuffdiff and should include hyphens(-)\n"+
        "if necessary.\n"
)
public class Cuffdiff extends ComputeBase {

    @Flag(shortForm = "-h",longForm = "--help")
    public boolean help;
    
    @Parameter(shortForm = "-in", longForm = "--input", description = "Multiple Transcript files (comma seperated)")
    public String in;
    
    @Parameter(shortForm = "-out", longForm= "--output", description = "output (directory)")
    public String out;
    
    @Parameter(shortForm = "-ref", longForm= "--ref_genome", description = "reference genome")
    public String ref;
    
    @Parameter(shortForm = "-assembly_opts", longForm = "--assembly_options", description = "options to pass to Cuffdiff (optional)")
    public String assembly_opts;
    
    @Parameter(shortForm = "-condn_labels", longForm = "--condition_labels", description = "Conditional labels to pass to Cuffdiff (optional)")
    public String condition_labels;
    
    @Parameter(shortForm = "-merged_gtf", longForm = "--merged_gtf", description = "Merged gtf file for Cuffdiff ")
    public String merged_gtf;
    
    @Parameter(shortForm = "-working_dir", longForm = "--working_dir", description = "workingdir (required)")
    public String working_dir;
    
    @Override
    public void handle(List<String> remainingArgs,Properties properties) throws Exception {

        super.handle(remainingArgs,properties);
        List<String> input = Arrays.asList(in.split(","));
        
        if(help){
            System.out.println(Utility.helpFromParameters(this.getClass()));
            return;
        }else if(null == in){
            System.out.println("missing -in parameter");
        }else if(null == out){
            System.out.println("missing -out parameter");
        }else if(null == merged_gtf){
            System.out.println("missing -merged_gtf parameter");
        }else if(fsclient.checkFileStatus(out, auth)){
        	System.out.println("ERROR : Output directory already exists");
        }else if(!fsclient.checkFileStatus(merged_gtf, auth)){
        	System.out.println("ERROR : Merged gtf does'nt exists");
        }else{
//            System.out.println("assembly_opts is " + assembly_opts + " in path is " +  in  + "outpath is " + out + "merged gtf " + merged_gtf + " workingdir is " + working_dir);
        	 for(String file : input) {
             	if(!fsclient.checkFileStatus(file, auth)){
             		System.out.println("ERROR : " + file + " does'nt exist");
             		return;
             	}
             }
        	JnomicsThriftJobID jobID = client.callCuffdiff(
                    in,
                    out,
                    ref,
                    Utility.nullToString(assembly_opts),
                    condition_labels,
                    merged_gtf,
                    Utility.nullToString(working_dir),
                    auth);
            System.out.println("Submitted Job: " + jobID.getJob_id());
            return;
        }

        System.out.println(Utility.helpFromParameters(this.getClass()));
    }
}
