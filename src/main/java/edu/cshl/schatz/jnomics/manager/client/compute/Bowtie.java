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

@FunctionDescription(description = "Align Reads using Bowtie Aligner\n"+
        "Short reads in .pe and .se format can be aligned to an organism\n"+
        "reference genome. Specify the organism via the -org flag.\n" +
        "Input and Output must reside in the Cluster's filesystem.\n"
)
public class Bowtie extends ComputeBase{

    @Flag(shortForm = "-h",longForm = "--help")
    public boolean help;

    @Parameter(shortForm = "-org", longForm = "--organism", description = "organism (index)")
    public String organism;

    @Parameter(shortForm = "-in", longForm = "--input", description = "input (directory,.pe,.se)")
    public String in;

    @Parameter(shortForm = "-out", longForm = "--output", description = "output directory")
    public String out;

    @Parameter(shortForm = "-opts", longForm = "--options", description="options to pass to bowtie")
    public String opts;


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
            JnomicsThriftJobID jobID = client.alignBowtie(in,
                    clean_org,
                    out,
                    Utility.nullToString(opts),
                    auth);

            System.out.println("Submitted Job: " + jobID.getJob_id());
            return;
        }

        System.out.println(Utility.helpFromParameters(this.getClass()));
    }
}
