package edu.cshl.schatz.jnomics.manager.client.compute;

import edu.cshl.schatz.jnomics.manager.api.Authentication;
import edu.cshl.schatz.jnomics.manager.api.JnomicsCompute;
import edu.cshl.schatz.jnomics.manager.client.ClientFunctionHandler;
import edu.cshl.schatz.jnomics.manager.client.old.JnomicsThriftClient;

import java.util.List;
import java.util.Properties;

/**
 * User: james
 */
public class ComputeBase implements ClientFunctionHandler {

    protected JnomicsCompute.Client client;
    
    protected Authentication auth;

    @Override
    public void handle(List<String> remainingArgs, Properties properties) throws Exception {
        client = JnomicsThriftClient.getComputeClient(properties);
        auth = JnomicsThriftClient.getAuthentication(properties);
    }
}
