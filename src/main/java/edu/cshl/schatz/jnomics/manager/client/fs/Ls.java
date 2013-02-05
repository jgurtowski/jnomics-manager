package edu.cshl.schatz.jnomics.manager.client.fs;

import edu.cshl.schatz.jnomics.manager.api.JnomicsThriftFileStatus;
import edu.cshl.schatz.jnomics.manager.client.ann.Flag;

import java.util.List;
import java.util.Properties;

/**
 * User: james
 */
public class Ls extends FSBase {

    @Flag(shortForm = "-h", longForm = "--help")
    public boolean help;

    @Override
    public void handle(List<String> remainingArgs, Properties properties) throws Exception {
        super.handle(remainingArgs,properties);

        if(help){
            System.out.println("-ls [directory]");
            return;
        }

        String dest = ".";
        if(remainingArgs.size() >= 1){
            dest = remainingArgs.get(0);
        }

        List<JnomicsThriftFileStatus> stats= client.listStatus(dest, auth);
        System.out.println("Found "+ stats.size() + " items");
        for(JnomicsThriftFileStatus status: stats){
            System.out.printf("%s\t%2d\t%s\t%s\t%14d\t%s\n",
                    status.getPermission(),
                    status.getReplication(),
                    status.getOwner(),
                    status.getGroup(),
                    status.getLength(),
                    status.getPath());
        }
    }

}
