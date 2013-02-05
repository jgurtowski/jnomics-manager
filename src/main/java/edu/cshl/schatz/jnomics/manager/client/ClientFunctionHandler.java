package edu.cshl.schatz.jnomics.manager.client;

import java.util.List;
import java.util.Properties;

/**
 * User: james
 */
public interface ClientFunctionHandler {
    public void handle(List<String> remainingArgs, Properties properties) throws Exception;
}
