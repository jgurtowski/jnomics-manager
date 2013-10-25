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


@FunctionDescription(description = "Tophat Aligner\n"+
        "Align Short reads to an organism's reference genome.\n"+
        "Organism can be specified with the -ref flag. Input and \n"+
        "Output must reside on the Cluster's filesystem. \n"+
        "Optional additonal arguments may be supplied to both\n"+
        "Tophat. These options are passed as a string to Tophat and should include hyphens(-)\n"+
        "if necessary.\n"
)
public class Tophat extends ComputeBase {

    @Flag(shortForm = "-h",longForm = "--help")
    public boolean help;
    
    @Parameter(shortForm = "-ref", longForm = "--reference genome", description="reference genome(.fa)")
    public String organism;
    
    @Parameter(shortForm = "-in", longForm = "--input", description = "input (directory,.pe,.se)")
    public String in;
    
    @Parameter(shortForm = "-out", longForm= "--output", description = "output (directory)")
    public String out;
    
    @Parameter(shortForm = "-align_opts", longForm = "--alignment_options", description = "options to pass to Tophat (optional)")
    public String align_opts;
    
    @Parameter(shortForm = "-gtf_opts", longForm = "--GTF file", description = "Gene model annotation file(.gtf) (optional)")
    public String gtf_opts;
    
    @Parameter(shortForm = "-working_dir", longForm = "--working_dir", description = "workingdir (required)")
    public String working_dir;
    
    @Override
    public void handle(List<String> remainingArgs,Properties properties) throws Exception {

        super.handle(remainingArgs,properties);
        List<String> input = Arrays.asList(in.split(","));
        if(help){
            System.out.println(Utility.helpFromParameters(this.getClass()));
            return;
        }else if(null == organism){
            System.out.println("missing -ref parameter");
        }else if(null == in){
            System.out.println("missing -in parameter");
        }else if(null == out){
            System.out.println("missing -out parameter");
    	}else if(fsclient.checkFileStatus(out, auth)){
    		System.out.println("ERROR : Output directory already exists");
    	}else{
            for(String file : input) {
            	if(!fsclient.checkFileStatus(file, auth)){
            		System.out.println("ERROR : " + file + " does'nt exist");
            		return;
            	}
            }
            String clean_org = KBaseIDTranslator.translate(organism);
           // System.out.println("organism  is " +  clean_org);
            JnomicsThriftJobID jobID = client.alignTophat(
                    clean_org,
                    in,
                    Utility.nullToString(gtf_opts),
                    out,
                    Utility.nullToString(align_opts),
                    Utility.nullToString(working_dir),
                    auth);
            System.out.println("Submitted Job: " + jobID.getJob_id());
            return;
            //}
        }

        System.out.println(Utility.helpFromParameters(this.getClass()));
    }
}
