package edu.cshl.schatz.jnomics.manager.client;

import edu.cshl.schatz.jnomics.manager.common.JnomicsApiConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

/**
 * User: james
 */

public class JnomicsKbaseClient {

    private static String [][] mainMenu = new String[][]{
            {new String("compute"), new String("Compute Functions")},
            {new String("fs"), new String("Filesystem Functions")}
    };

    private static void printMainMenu(){
        for(String []item : mainMenu){
            System.out.println(String.format("%-30s %30s", item[0], item[1]));
        }
    }
    
    public static void main(String []args) throws Exception {

        if(args.length < 1){
            printMainMenu();
            System.exit(-1);
        }

        Properties prop = JnomicsApiConfig.getClientProperties();
        
        ClientHandler ch = null;
        
        if(0 == args[0].compareTo("compute")){
            ch = new ClientComputeHandler();
        }else if(0 == args[0].compareTo("fs")){
            ch = new ClientFSHandler(prop);
        }else{
            printMainMenu();
            System.exit(-1);
        }

        ArrayList<String> newArgs = new ArrayList<String>(Arrays.asList(args));
        newArgs.remove(args[0]);
        ch.handle(newArgs);
    }
}
