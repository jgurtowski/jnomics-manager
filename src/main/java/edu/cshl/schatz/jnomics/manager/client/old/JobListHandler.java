package edu.cshl.schatz.jnomics.manager.client.old;

import edu.cshl.schatz.jnomics.manager.api.Authentication;
import edu.cshl.schatz.jnomics.manager.api.JnomicsCompute;
import edu.cshl.schatz.jnomics.manager.api.JnomicsThriftJobStatus;

import java.util.List;
import java.util.Properties;

/**
 * User: james
 */

public class JobListHandler extends HandlerBase {

    public JobListHandler(){

    }

    @Override
    public void handle(List<String> args, Properties properties) throws Exception {
        JnomicsCompute.Client client = JnomicsThriftClient.getComputeClient(properties);
        Authentication auth = JnomicsThriftClient.getAuthentication(properties);

        for(JnomicsThriftJobStatus status : client.getAllJobs(auth)){
            System.out.println(status);
        }
    }

    @Override
    public String getDescription() {
        return "List all Jobs";
    }
}