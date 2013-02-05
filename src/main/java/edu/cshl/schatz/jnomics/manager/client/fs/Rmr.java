package edu.cshl.schatz.jnomics.manager.client.fs;

import edu.cshl.schatz.jnomics.manager.client.ann.Flag;

import java.util.List;
import java.util.Properties;

/**
 * User: james
 */
public class Rmr extends FSBase{

    @Flag(shortForm = "-h",longForm = "--help")
    public boolean help;

    @Override
    public void handle(List<String> remainingArgs, Properties properties) throws Exception {
        super.handle(remainingArgs, properties);
        if(remainingArgs.size() < 1 || help){
            System.out.println("-rm <directory>");
        }else{
            boolean recurse = true;
            boolean status;
            for(String file : remainingArgs){
                status = client.remove(file,recurse,auth);
                if(status)
                    System.out.println("Deleted: " + file);
                else
                    System.out.println("Failed Deleting: " + file);
            }
        }
    }
}
