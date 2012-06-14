package edu.cshl.schatz.jnomics.manager.client;

import edu.cshl.schatz.jnomics.manager.api.*;
import org.apache.thrift.TException;

/**
 * User: james
 */
public class JnomicsThriftPoller {
    
    public static int pollForCompletion(JnomicsThriftJobID id, Authentication auth, JnomicsCompute.Client client)
            throws JnomicsThriftException, TException {
        
        System.out.print("Polling Job "+ id);
        JnomicsThriftJobStatus status = client.getJobStatus(id, auth);

        while(status.getRunning_state() != 1){
            if(status.getRunning_state() == 3)
                throw new JnomicsThriftException("Job failed");
            try {
                Thread.sleep(1000 * 60 * 5);
                System.out.print(".");
            } catch (InterruptedException e) {
                throw new JnomicsThriftException(e.getMessage());
            }
        }
        
        System.out.println("done");
        return status.getRunning_state();
    }
}
