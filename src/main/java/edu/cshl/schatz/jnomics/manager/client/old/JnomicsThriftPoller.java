package edu.cshl.schatz.jnomics.manager.client.old;

import edu.cshl.schatz.jnomics.manager.api.*;
import org.apache.thrift.TException;

/**
 * User: james
 */
public class JnomicsThriftPoller {
    
    public static int pollForCompletion(JnomicsThriftJobID id, Authentication auth, JnomicsCompute.Client client)
            throws JnomicsThriftException, TException {
        
        System.out.println("Polling Job " + id.getJob_id());
        JnomicsThriftJobStatus status = client.getJobStatus(id, auth);

        while(!status.isComplete()){
            System.out.println("Map: " +
                    (int)(status.getMapProgress() * 100) +
                    "% Reduce: " +
                    (int)(status.getReduceProgress() * 100) +
                    "%"
            );
            try {
                Thread.sleep(1000 * 60 * 2);
            } catch (InterruptedException e) {
                throw new JnomicsThriftException(e.toString());
            }
            status = client.getJobStatus(id, auth);

        }

        if(status.getRunning_state() != 2)
            throw new JnomicsThriftException(status.getFailure_info());

        return status.getRunning_state();
    }
}
