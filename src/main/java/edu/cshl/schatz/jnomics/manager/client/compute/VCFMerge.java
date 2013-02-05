package edu.cshl.schatz.jnomics.manager.client.compute;

import edu.cshl.schatz.jnomics.manager.client.Utility;
import edu.cshl.schatz.jnomics.manager.client.ann.Flag;
import edu.cshl.schatz.jnomics.manager.client.ann.FunctionDescription;
import edu.cshl.schatz.jnomics.manager.client.ann.Parameter;

import java.util.List;
import java.util.Properties;

/**
 * User: james
 */

@FunctionDescription(description = "The Variation pipelines are run in parallel.\n"+
        "When they complete, many VCF files will be produced that must be merged\n"+
        "This command takes the output from a variation pipeline and metadata from\n"+
        "the alignment operations and produces a merged VCF file.\n"+
        "The output will be a single VCF file in the Cluster's filesystem.\n"+
        "The file can be downloaded with fs -get\n"
)
public class VCFMerge extends ComputeBase {

    @Flag(shortForm = "-h", longForm = "--help", description = "This Help")
    public boolean help;
    
    @Parameter(shortForm = "-in", longForm = "--input", description = "input (directory of vcfs)")
    public String in;
    
    @Parameter(shortForm = "-aln", longForm = "--alignments", description = "alignments (directory)")
    public String alignments;
    
    @Parameter(shortForm = "-out", longForm = "--output", description = "output (vcf file)")
    public String out;
    
    @Override
    public void handle(List<String> remainingArgs, Properties properties) throws Exception {
        super.handle(remainingArgs, properties);

        if(help){
            System.out.println(Utility.helpFromParameters(this.getClass()));
            return;
        }else if(null == in){
            System.out.println("Missing -in parameter");
        }else if(null == alignments){
            System.out.println("Missing -aln parameter");
        }else if(null == out){
            System.out.println("Missing -out parameter");
        }else{
            if(client.mergeVCF(in,
                    alignments,
                    out,
                    auth)){
                System.out.println("Successfully merged into : " + out);
            }else{
                System.out.println("Failed to Merge : "+  out);
            }
            return;
        }

        System.out.println(Utility.helpFromParameters(this.getClass()));
    }
}
