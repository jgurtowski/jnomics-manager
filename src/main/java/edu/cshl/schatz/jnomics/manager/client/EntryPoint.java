package edu.cshl.schatz.jnomics.manager.client;

import edu.cshl.schatz.jnomics.manager.client.ann.Flag;
import edu.cshl.schatz.jnomics.manager.client.ann.FunctionDescription;
import edu.cshl.schatz.jnomics.manager.client.ann.Parameter;
import edu.cshl.schatz.jnomics.manager.common.JnomicsApiConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * User: james
 */
public class EntryPoint {

    @FunctionDescription(description = "Use the functions below to interact with the Cluster")
    public static class EntryHandler implements ClientFunctionHandler {

        @Flag(shortForm = "fs", longForm = "filesystem", description = "Commands that interact with the Cluster Filesystem")
        public boolean fs;
        
        @Flag(shortForm = "compute", longForm = "compute", description = "Commands that launch Compute tasks on the Cluster")
        public boolean compute;

        @Parameter(shortForm = "-user",longForm = "--username", description = "Optionally specify username via commandline")
        public String username;
        
        @Parameter(shortForm = "-pass", longForm = "--password", description = "Optionally specify password via commandline")
        public String password;


        @Override
        public void handle(List<String> remainingArgs, Properties properties) throws Exception {
            CreatedHandler createdHandler = null;
            if(null != username && null != password){
                String token = GlobusPasswordPrompter.getTokenForUser(username, password);
                properties.setProperty("username",username);
                properties.setProperty("password",password);
                properties.setProperty("token", token);

            }else if(null != System.getenv("KB_AUTH_TOKEN")){
                properties.setProperty("token",System.getenv("KB_AUTH_TOKEN"));
            }
            
            JnomicsApiConfig.getClientProperties(properties);

            if(fs){
                createdHandler = Utility.handlerFromArgs(remainingArgs,FS.class);
            }else if(compute){
                createdHandler = Utility.handlerFromArgs(remainingArgs, Compute.class);
            }else{
                System.out.println(Utility.helpFromParameters(this.getClass()));
            }

            if(null != createdHandler){
                createdHandler.getHandler().handle(createdHandler.getRemainingArgs(),properties);
            }
        }
    }


    public static void main(String []args) throws Exception {
        List<String> largs = new ArrayList<String>(Arrays.asList(args));
        CreatedHandler createdHandler = Utility.handlerFromArgs(largs,EntryHandler.class);
        createdHandler.getHandler().handle(createdHandler.getRemainingArgs(),new Properties());
    }

}
