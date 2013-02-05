package edu.cshl.schatz.jnomics.manager.client.old;

import edu.cshl.schatz.jnomics.manager.api.Authentication;
import edu.cshl.schatz.jnomics.manager.api.JnomicsCompute;
import edu.cshl.schatz.jnomics.manager.api.JnomicsData;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;

import java.util.List;
import java.util.Properties;

/**
 * User: james
 */

public class SamtoolsPipelineHandler extends HandlerBase {

    public SamtoolsPipelineHandler(){

    }

    public JnomicsArgument[] getArguments(){
        return new JnomicsArgument[]{
                new JnomicsArgument("in",true,true,"Input raw reads"),
                new JnomicsArgument("out",true,true,"Output dir - for entire samtools pipeline"),
                new JnomicsArgument("organism",true,true,"Organism for alignment index")
        };
    }

    @Override
    public void handle(List<String> args, Properties properties) throws Exception {
        HelpFormatter formatter = new HelpFormatter();
        Options options = getOptions();
        CommandLine cli = null;
        try{
            cli = parseArguments(args);
        }catch(JnomicsArgumentException e){
            formatter.printHelp(e.toString(),options);
            return;
        }

        JnomicsData.Client dataClient = JnomicsThriftClient.getFsClient(properties);
        Authentication auth = JnomicsThriftClient.getAuthentication(properties);

        String in = cli.getOptionValue("in");
        String out = cli.getOptionValue("out");
        String organism = cli.getOptionValue("organism");
        
        if (dataClient.listStatus(out,auth).size() > 0)
            throw new Exception("Output Directory already exists");
        dataClient.mkdir(out,auth);

        //JnomicsCompute.AsyncClient computeClient = JnomicsThriftClient.getAsyncComputeClient();
        JnomicsCompute.Client computeClient = JnomicsThriftClient.getComputeClient(properties);

        computeClient.runSNPPipeline(in,organism,out,auth);
        /*computeClient.runSNPPipeline(in,organism,out,auth,new AsyncMethodCallback<JnomicsCompute.AsyncClient.runSNPPipeline_call>() {
            @Override
            public void onComplete(JnomicsCompute.AsyncClient.runSNPPipeline_call response) {
            }

            @Override
            public void onError(Exception exception) {
            }
        });*/

        /*System.out.println("Aligning Reads");
        String alignDir = out+"/realign";
        JnomicsThriftPoller.pollForCompletion(
                computeClient.alignBowtie(in,organism,alignDir,"",auth),
                auth,
                computeClient
        );

        System.out.println("Calling Variants");
        String variants = out+"/variants";
        JnomicsThriftPoller.pollForCompletion(
                computeClient.snpSamtools(alignDir,organism,variants,auth),
                auth,
                computeClient
        );*/
        System.out.println("Final Output will be in:" + new Path(out,"out.vcf").toString());
    }

    @Override
    public String getDescription() {
        return "Run Alignment/Samtools Pipeline";
    }
}