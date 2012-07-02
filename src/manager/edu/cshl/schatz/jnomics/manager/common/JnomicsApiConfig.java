package edu.cshl.schatz.jnomics.manager.common;

import java.io.IOException;
import java.util.Properties;

/**
 * User: james
 */
public class JnomicsApiConfig {

    private static String SERVER_PROP_FILE = "jnomics-kbase-server.properties";
    private static String CLIENT_PROP_FILE = "jnomics-kbase-client.properties";
    
    public static Properties getClientProperties() throws IOException {
        Properties prop = new Properties();
        try{
            prop.load(ClassLoader.getSystemResourceAsStream(CLIENT_PROP_FILE));
        }catch(Exception e){
            throw new IOException("Could not find " + CLIENT_PROP_FILE);
        }
        return prop;
    }
    
    public static Properties getServerProperties() throws IOException{
        Properties prop = new Properties();
        try{
            prop.load(ClassLoader.getSystemResourceAsStream(SERVER_PROP_FILE));
        }catch(Exception e){
            throw new IOException("Could not find " + SERVER_PROP_FILE);
        }
        return prop;
    }
}
