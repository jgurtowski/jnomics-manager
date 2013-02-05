package edu.cshl.schatz.jnomics.manager.client.fs;

import edu.cshl.schatz.jnomics.manager.client.ann.Flag;

import java.util.List;
import java.util.Properties;

/**
 * User: james
 */
public class Mv extends FSBase{

    @Flag(shortForm = "-h", longForm = "--help")
    public boolean help;

    @Override
    public void handle(List<String> remainingArgs, Properties properties) throws Exception {
        super.handle(remainingArgs, properties);

        if(remainingArgs.size() < 2 || help){
            System.out.println("fs -mv <dir/file> <dest>");
            return;
        }

        if(client.mv(remainingArgs.get(0), remainingArgs.get(1), auth)){
            System.out.println("Moved " + remainingArgs.get(0) + " to " + remainingArgs.get(1));
        }else{
            System.out.println("Failed to move "+remainingArgs.get(0)+" to "+remainingArgs.get(1));
        }
    }
}
