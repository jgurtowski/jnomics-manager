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

@FunctionDescription(description = "Samtools SNP\n"+
        "Run Samtools SNP pipeline on aligned reads\n"+
        "The general workflow includes uploading short reads\n"+
        "to the Cluster. Aligning them with a given Aligner.\n"+
        "The Samtools SNP algorithm can then be called on the output\n" +
        "from the alignment step. The organism reference genome\n"+
        "should be supplied with the -org paramter.\n"
)
public class SamtoolsSnp extends ComputeBase{

    @Flag(shortForm = "-h", longForm = "--help", description = "This Help")
    public boolean help;

    @Parameter(shortForm = "-org", longForm = "--organism", description = "Organism reference genome")
    public String organism;
    
    @Parameter(shortForm = "-in", longForm = "--input", description = "Input path on the Cluster (Alignments)")
    public String input;
    
    @Parameter(shortForm = "-out", longForm = "--output", description = "Output path on the Cluster")
    public String output;


    @Override
    public void handle(List<String> remainingArgs, Properties properties) throws Exception {
        super.handle(remainingArgs, properties);

        if(help){
            System.out.println(Utility.helpFromParameters(this.getClass()));
            return;
        }else if(null == organism){
            System.out.println("missing -org parameter");
        }else if(null == input){
            System.out.println("missing -in parameter");
        }else if(null == output){
            System.out.println("missing -out parameter");
        }else{
            String clean_org = KBaseIDTranslator.translate(organism);
            JnomicsThriftJobID jobID = client.snpSamtools(input,
                    clean_org,
                    output,
                    auth);
            System.out.println("Submitted Job: " + jobID.getJob_id());
            return;
        }

        System.out.println(Utility.helpFromParameters(this.getClass()));

    }
}
