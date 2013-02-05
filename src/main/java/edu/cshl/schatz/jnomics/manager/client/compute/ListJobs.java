package edu.cshl.schatz.jnomics.manager.client.compute;

import edu.cshl.schatz.jnomics.manager.api.JnomicsThriftJobStatus;

import java.util.List;
import java.util.Properties;

/**
 * User: james
 */

public class ListJobs extends ComputeBase {

    @Override
    public void handle(List<String> remainingArgs, Properties properties) throws Exception {
        super.handle(remainingArgs, properties);
        
        List<JnomicsThriftJobStatus> jobs = client.getAllJobs(auth);

        if(jobs.size() == 0){
            System.out.println("No Active Jobs.");
        }else{
            for(JnomicsThriftJobStatus status : jobs){
                System.out.println(status);
            }
        }
    }
}
