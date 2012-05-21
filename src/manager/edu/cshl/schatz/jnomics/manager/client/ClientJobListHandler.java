package edu.cshl.schatz.jnomics.manager.client;

import edu.cshl.schatz.jnomics.manager.api.JnomicsThriftJobStatus;

import java.util.List;
import java.util.Properties;

/**
 * User: james
 */

public class ClientJobListHandler extends ClientThriftHandler {

    public ClientJobListHandler(Properties properties) {
        super(properties);
    }

    @Override
    public void handle(List<String> args) throws Exception {
        for(JnomicsThriftJobStatus status : getThriftClient().getAllJobs(getAuth())){
            System.out.println(status);
        }
        closeTransport();
    }

}