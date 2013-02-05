package edu.cshl.schatz.jnomics.manager.client.old;

import edu.cshl.schatz.jnomics.manager.api.JnomicsThriftException;
import edu.cshl.schatz.jnomics.manager.client.GlobusPasswordPrompter;
import edu.cshl.schatz.jnomics.manager.common.JnomicsApiConfig;

import java.util.*;

/**
 * User: james
 */

public class Entry {

    private static String [][] mainMenu = new String[][]{
            {new String("compute"), new String("Compute Functions")},
            {new String("fs"), new String("Filesystem Functions")},
            {new String("[-username]"), new String("Optional Username")},
            {new String("[-password]"), new String("Optional Password")}
    };

    private static void printMainMenu(){
        for(String []item : mainMenu){
            System.out.println(String.format("%-30s %30s", item[0], item[1]));
        }
    }

    /**
     * @param args - cli args to process
     * @param prop - properties to populate
     * @return modified args list with the username/password fields removed
     */
    private static String[] addCredsToProp(String []args, Properties prop){
        String username = null;
        String password = null;

        List<String> arr = new ArrayList<String>(Arrays.asList(args));
        for(int i=0;i<arr.size();i++){
            if(arr.get(i).compareTo("-username") == 0 && i != arr.size()-1){
                username = arr.get(i+1);
                arr.remove(i+1);
                arr.remove(i);
                break;
            }
        }
        for(int j=0;j<arr.size();j++){
            if(arr.get(j).compareTo("-password") == 0 && j != arr.size()-1){
                password = arr.get(j+1);
                arr.remove(j+1);
                arr.remove(j);
            }
        }
        arr.remove("-password");
        arr.remove("-username");

        if(username != null && password != null){
            String token = null;
            try {
                token = GlobusPasswordPrompter.getTokenForUser(username,password);
            } catch (Exception e) {
                System.out.println(e.getMessage());
                System.exit(-1);
            }
            prop.setProperty("username",username);
            prop.setProperty("password",password);
            prop.setProperty("token", token);
        }

        return arr.toArray(new String[0]);
    }
    
    public static void main(String []args) throws Exception {
        if(args.length < 1){
            printMainMenu();
            System.exit(-1);
        }
                
        Properties prop = new Properties();

        //if user/password is specified on the commandline,
        //add them to the properties list
        String []new_args = addCredsToProp(args,prop);
        JnomicsApiConfig.getClientProperties(prop);
        HandlerBase ch = null;
        
        if(0 == new_args[0].compareTo("compute")){
            ch = new ComputeHandler();
        }else if(0 == new_args[0].compareTo("fs")){
            ch = new FSHandler();
        }else{
            printMainMenu();
            System.exit(-1);
        }

        ArrayList<String> newArgs = new ArrayList<String>(Arrays.asList(new_args));
        newArgs.remove(new_args[0]);
        try{
            ch.handle(newArgs,prop);
        }catch(JnomicsThriftException e){
            if(e.msg.compareTo("Permission Denied") == 0){
                GlobusPasswordPrompter.getPasswordFromUser();
                main(args);
            }else{
                throw e;
            }
        }
    }
}
