package edu.cshl.schatz.jnomics.manager.client.compute;

import edu.cshl.schatz.jnomics.manager.api.JnomicsThriftJobID;
import edu.cshl.schatz.jnomics.manager.client.Utility;
import edu.cshl.schatz.jnomics.manager.client.ann.Flag;
import edu.cshl.schatz.jnomics.manager.client.ann.FunctionDescription;
import edu.cshl.schatz.jnomics.manager.client.ann.Parameter;
import edu.cshl.schatz.jnomics.manager.common.KBaseIDTranslator;

import java.util.List;
import java.util.Properties;

/**
 * User: james
 */


@FunctionDescription(description = "BWA Aligner\n"+
        "Align Short reads to an organism's reference genome.\n"+
        "Organism can be specified with the -org flag. Input and \n"+
        "Output must reside on the Cluster's filesystem. \n"+
        "Optional additonal arguments may be supplied to both bwa aln and\n"+
        "bwa sampe. These options are passed as a string to bwa and should include hyphens(-)\n"+
        "if necessary.\n"
)
public class BWA extends ComputeBase {

    @Flag(shortForm = "-h",longForm = "--help")
    public boolean help;
    
    @Parameter(shortForm = "-org", longForm = "--organism", description="organism (index)")
    public String organism;
    
    @Parameter(shortForm = "-in", longForm = "--input", description = "input (directory,.pe,.se)")
    public String in;
    
    @Parameter(shortForm = "-out", longForm= "--output", description = "output (directory)")
    public String out;
    
    @Parameter(shortForm = "-align_opts", longForm = "--alignment_options", description = "options to pass to bwa aln")
    public String align_opts;
    
    @Parameter(shortForm = "-sampe_opts", longForm = "--sampe_options", description = "options to pass to bwa sampe")
    public String sampe_opts;
    
    @Override
    public void handle(List<String> remainingArgs,Properties properties) throws Exception {

        super.handle(remainingArgs,properties);
        if(help){
            System.out.println(Utility.helpFromParameters(this.getClass()));
            return;
        }else if(null == organism){
            System.out.println("missing -org parameter");
        }else if(null == in){
            System.out.println("missing -in parameter");
        }else if(null == out){
            System.out.println("missing -out parameter");
        }else{
            String clean_org = KBaseIDTranslator.translate(organism);
            JnomicsThriftJobID jobID = client.alignBWA(
                    in,
                    clean_org,
                    out,
                    Utility.nullToString(align_opts),
                    Utility.nullToString(sampe_opts),
                    auth);
            System.out.println("Submitted Job: " + jobID.getJob_id());
            return;
        }

        System.out.println(Utility.helpFromParameters(this.getClass()));
    }
}
