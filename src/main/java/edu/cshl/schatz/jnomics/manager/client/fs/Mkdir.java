package edu.cshl.schatz.jnomics.manager.client.fs;

import edu.cshl.schatz.jnomics.manager.client.ann.Flag;

import java.util.List;
import java.util.Properties;

/**
 * User: james
 */
public class Mkdir extends FSBase{

    @Flag(shortForm = "-h", longForm = "--help")
    public boolean help;

    @Override
    public void handle(List<String> remainingArgs, Properties properties) throws Exception {
        super.handle(remainingArgs, properties);

        if(remainingArgs.size() < 1 || help){
            System.out.println("fs -mkdir <directory>");
            return;
        }
        if(client.mkdir(remainingArgs.get(0), auth)){
            System.out.println("Mkdir: " + remainingArgs.get(0));
        }else{
            System.out.println("Failed to mkdir: " + remainingArgs.get(0));
        }
    }
}
