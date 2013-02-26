package edu.cshl.schatz.jnomics.manager.client.fs;

import edu.cshl.schatz.jnomics.manager.api.Authentication;
import edu.cshl.schatz.jnomics.manager.api.JnomicsData;
import edu.cshl.schatz.jnomics.manager.client.ClientFunctionHandler;
import edu.cshl.schatz.jnomics.manager.client.old.JnomicsThriftClient;

import java.util.List;
import java.util.Properties;

/**
 * User: james
 */
public class FSBase implements ClientFunctionHandler {

    protected Authentication auth;
    protected JnomicsData.Client client;
    
    @Override
    public void handle(List<String> remainingArgs, Properties properties) throws Exception {
        client = JnomicsThriftClient.getFsClient(properties);
        String username = properties.getProperty("username");
        String password = properties.getProperty("password");
        String token = properties.getProperty("token");
        auth = new Authentication(username,password,token);

    }
}
