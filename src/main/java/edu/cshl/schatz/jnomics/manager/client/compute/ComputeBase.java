package edu.cshl.schatz.jnomics.manager.client.compute;

import edu.cshl.schatz.jnomics.manager.api.Authentication;
import edu.cshl.schatz.jnomics.manager.api.JnomicsCompute;
import edu.cshl.schatz.jnomics.manager.api.JnomicsData;
import edu.cshl.schatz.jnomics.manager.client.ClientFunctionHandler;
import edu.cshl.schatz.jnomics.manager.client.old.JnomicsThriftClient;
import edu.cshl.schatz.jnomics.manager.server.JnomicsFileSystem;
import edu.cshl.schatz.jnomics.manager.server.JnomicsFsHandle;

import java.util.List;
import java.util.Properties;

/**
 * User: james
 */
public class ComputeBase implements ClientFunctionHandler {

    protected JnomicsCompute.Client client;
    protected JnomicsData.Client fsclient;
    protected Authentication auth;

    @Override
    public void handle(List<String> remainingArgs, Properties properties) throws Exception {
        client = JnomicsThriftClient.getComputeClient(properties);
        fsclient = JnomicsThriftClient.getFsClient(properties);
        auth = JnomicsThriftClient.getAuthentication(properties);
    }
}
