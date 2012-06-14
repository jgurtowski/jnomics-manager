package edu.cshl.schatz.jnomics.manager.client;

import edu.cshl.schatz.jnomics.manager.api.Authentication;
import edu.cshl.schatz.jnomics.manager.api.JnomicsCompute;
import edu.cshl.schatz.jnomics.manager.api.JnomicsData;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import java.util.List;

/**
 * User: james
 */

public class ClientGatkHandler extends ClientHandler{

    public ClientGatkHandler(){

    }

    public JnomicsArgument[] getArguments(){
        return new JnomicsArgument[]{
                new JnomicsArgument("in",true,true,"Input aligned reads"),
                new JnomicsArgument("out",true,true,"Output dir - for entire gatk pipeline"),
                new JnomicsArgument("organism",true,true,"Organism for alignment index")
        };
    }

    @Override
    public void handle(List<String> args) throws Exception {


        HelpFormatter formatter = new HelpFormatter();
        Options options = getOptions();
        CommandLine cli = null;
        try{
            cli = parseArguments(args);
        }catch(JnomicsArgumentException e){
            formatter.printHelp(e.toString(),options);
            return;
        }

        JnomicsData.Client dataClient = JnomicsThriftClient.getFsClient();
        Authentication auth = JnomicsThriftClient.getAuthentication();

        String in = cli.getOptionValue("in");
        String out = cli.getOptionValue("out");
        String organism = cli.getOptionValue("organism");
        
        if (dataClient.listStatus(out,auth).size() > 0)
            throw new Exception("Output Directory already exists");
        dataClient.mkdir(out,auth);
        JnomicsCompute.Client computeClient = JnomicsThriftClient.getComputeClient();
        
        
        System.out.println("Realigning Reads");
        String realignDir = out+"/realign";
        JnomicsThriftPoller.pollForCompletion(
                computeClient.gatkRealign(in, organism, realignDir, auth),
                auth,
                computeClient
        );

        System.out.println("Calling Initial Variants");
        String variants_one = out+"/variants_one";
        JnomicsThriftPoller.pollForCompletion(
                computeClient.gatkCallVariants(realignDir,organism,variants_one,auth),
                auth,
                computeClient
        );

        System.out.println("Merging Variant Calls");
        String variants_vcf_one = out+"/variants_one.vcf";
        computeClient.mergeVCF(variants_one,variants_vcf_one,auth);

        System.out.println("Counting Covariates");
        String covariatesDir = out+"/covariates";
        JnomicsThriftPoller.pollForCompletion(
                computeClient.gatkCountCovariates(realignDir, organism, variants_vcf_one, covariatesDir, auth),
                auth,
                computeClient
        );

        System.out.println("Merging Covariates");
        String covariatesMerge = out+"/covariates.cov";
        computeClient.mergeCovariate(covariatesDir,covariatesMerge,auth);
        
        System.out.println("Recalibrating");
        String recalDir = out+"/recalibrate";
        JnomicsThriftPoller.pollForCompletion(
                computeClient.gatkRecalibrate(realignDir,organism, covariatesMerge,recalDir,auth),
                auth,
                computeClient
        );

        System.out.println("Calling Final Variants");
        String variants_final = out+"/variants_final";
        JnomicsThriftPoller.pollForCompletion(
                computeClient.gatkCallVariants(recalDir,organism,variants_final,auth),
                auth,
                computeClient
        );
        
        System.out.println("Merging Final Variant Class");
        String variants_vcf_final = out+"/variants_final.vcf";
        computeClient.mergeVCF(variants_final,variants_vcf_final,auth);
    }

    @Override
    public String getDescription() {
        return "Run GATK Pipeline";
    }
}