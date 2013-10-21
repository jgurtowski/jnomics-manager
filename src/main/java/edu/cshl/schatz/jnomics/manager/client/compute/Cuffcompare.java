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
import java.util.List;
import java.util.Properties;

/**
 * User: Sri
 */


@FunctionDescription(description = "Cuffcompare Transcript Assembler\n"+
        "Align Short reads to an organism's reference genome.\n"+
        "Organism can be specified with the -org flag. Input and \n"+
        "Output must reside on the Cluster's filesystem. \n"+
        "Optional additonal arguments may be supplied to both\n"+
        "cufflinks. These options are passed as a string to cufflinks and should include hyphens(-)\n"+
        "if necessary.\n"
)
public class Cuffcompare extends ComputeBase {

    @Flag(shortForm = "-h",longForm = "--help")
    public boolean help;
    
    @Parameter(shortForm = "-in", longForm = "--input", description = "input (directory,.pe,.se)")
    public String in;
    
    @Parameter(shortForm = "-out", longForm= "--output", description = "output (directory)")
    public String out;
    
    @Parameter(shortForm = "-ref_gtf", longForm= "--ref_gtf", description = "reference gtf")
    public String ref_gtf;
    
    @Parameter(shortForm = "-assembly_opts", longForm = "--assembly_options", description = "options to pass to Cufflinks (optional)")
    public String assembly_opts;
    
    @Parameter(shortForm = "-working_dir", longForm = "--working_dir", description = "workingdir (required)")
    public String working_dir;
    
    @Override
    public void handle(List<String> remainingArgs,Properties properties) throws Exception {

        super.handle(remainingArgs,properties);
        if(help){
            System.out.println(Utility.helpFromParameters(this.getClass()));
            return;
        }else if(null == in){
            System.out.println("missing -in parameter");
        }else if(null == out){
            System.out.println("missing -out parameter");
        }else if(null == ref_gtf){
            System.out.println("missing -ref_gtf parameter");    
        }else{
//            String clean_org = KBaseIDTranslator.translate(organism);
//            List<JnomicsThriftFileStatus> stats  = client.listStatus(in, auth);
//            StringBuilder sb = new StringBuilder();
//            for(String opts : align_opts){
//            	sb.append(" " + opts);
//            }
            System.out.println("Assebly_opts : " + assembly_opts + " Input :  " + in + " Outpath : " + out );
            JnomicsThriftJobID jobID = client.callCuffcompare(
                    in,
                    out,
                    ref_gtf,
                    Utility.nullToString(assembly_opts),
                    Utility.nullToString(working_dir),
                    auth);
            System.out.println("Submitted Job: " + jobID.getJob_id());
            return;
        }

        System.out.println(Utility.helpFromParameters(this.getClass()));
    }
}
