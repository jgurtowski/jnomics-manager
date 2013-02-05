package edu.cshl.schatz.jnomics.manager.client.old;

import org.apache.commons.cli.*;

import java.util.List;
import java.util.Properties;

/**
 * User: james
 */
public abstract class HandlerBase {

    private final Options options = new Options();

    public abstract void handle(List<String> args, Properties properties) throws Exception;

    public String getDescription(){
        return "";
    }
    
    /**
     * Override to inject arguments
     */
    protected JnomicsArgument[] getArguments(){
        return new JnomicsArgument[0];
    }

    public HandlerBase(){
        //setup arguments
        for(JnomicsArgument arg: getArguments()){
            options.addOption(arg.getName(), arg.hasArguments(), arg.getDescription());
        }
    }


    protected CommandLine parseArguments(List<String> args) throws ParseException, JnomicsArgumentException {
        BasicParser parser = new BasicParser();

        CommandLine cli = parser.parse(options, args.toArray(new String[0]),false);

        for(JnomicsArgument arg : getArguments()){
            if(arg.isRequired() && !cli.hasOption(arg.getName()))
                throw new JnomicsArgumentException("Missing " + arg.getName());
        }

        return cli;
    }

    protected Options getOptions(){
        return options;
    }
    
    protected static String nullToString(String n){
        return n == null ? "" : n;
    }
}
