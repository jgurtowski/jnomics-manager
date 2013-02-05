package edu.cshl.schatz.jnomics.manager.client;

import java.util.LinkedList;
import java.util.List;

/**
 * User: james
 */
public class CreatedHandler {

    ClientFunctionHandler handler;
    List<String> remainingArgs;

    public CreatedHandler(ClientFunctionHandler handler,
                          List<String> remainingArgs){
        this.handler = handler;
        this.remainingArgs = remainingArgs;
    }

    public ClientFunctionHandler getHandler() {
        return handler;
    }

    public void setHandler(ClientFunctionHandler handler) {
        this.handler = handler;
    }

    public List<String> getRemainingArgs() {
        return remainingArgs;
    }

    public void setRemainingArgs(LinkedList<String> remainingArgs) {
        this.remainingArgs = remainingArgs;
    }
}
