package edu.cshl.schatz.jnomics.manager.client;

import edu.cshl.schatz.jnomics.manager.api.Authentication;
import edu.cshl.schatz.jnomics.manager.api.JnomicsCompute;
import edu.cshl.schatz.jnomics.manager.api.JnomicsThriftJobStatus;

import java.util.List;

/**
 * User: james
 */

public class ClientJobListHandler extends ClientHandler {

    public ClientJobListHandler(){

    }

    @Override
    public void handle(List<String> args) throws Exception {
        JnomicsCompute.Client client = JnomicsThriftClient.getComputeClient();
        Authentication auth = JnomicsThriftClient.getAuthentication();

        for(JnomicsThriftJobStatus status : client.getAllJobs(auth)){
            System.out.println(status);
        }
    }

    @Override
    public String getDescription() {
        return "List all Jobs";
    }
}