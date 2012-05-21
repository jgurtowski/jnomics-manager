package edu.cshl.schatz.jnomics.manager.common;

import java.io.IOException;
import java.util.Properties;

/**
 * User: james
 */
public class JnomicsApiConfig {

    private static String MAIN_PROP_FILE = "jnomics-kbase.properties";

    public static Properties get() throws IOException {
        Properties prop = new Properties();
        try{
            prop.load(ClassLoader.getSystemResourceAsStream(MAIN_PROP_FILE));
        }catch(Exception e){
            throw new IOException("Could not find " + MAIN_PROP_FILE);
        }
        return prop;
    }
}
