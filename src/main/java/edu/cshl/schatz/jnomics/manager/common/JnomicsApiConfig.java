package edu.cshl.schatz.jnomics.manager.common;

import edu.cshl.schatz.jnomics.manager.client.GlobusPasswordPrompter;
import edu.cshl.schatz.jnomics.manager.client.JnomicsClientEnvironment;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import us.kbase.auth.AuthService;

/**
 * User: james
 */
public class JnomicsApiConfig {

    private static String SERVER_PROP_FILE = "jnomics-kbase-server.properties";
    private static String CLIENT_PROP_FILE = "jnomics-kbase-client.properties";

    public static void getClientProperties(Properties prop) throws Exception {
        try{
            prop.load(ClassLoader.getSystemResourceAsStream(CLIENT_PROP_FILE));
        }catch(Exception e){
            throw new IOException("Could not find " + CLIENT_PROP_FILE);
        }
        if(prop.getProperty("token") == null){
            File authFile = JnomicsClientEnvironment.USER_AUTH_FILE;
            try{
                prop.load(new FileInputStream(authFile));
		//can throw TokenExpiredException in which case re-prompt the user
		AuthService.validateToken(prop.getProperty("token"));
            }catch(Exception e){
                GlobusPasswordPrompter.getPasswordFromUser();
                prop.load(new FileInputStream(authFile));
            }
	}
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

    public static void setHTTPProxy(String http_proxy) throws IOException{

	if(null != http_proxy && http_proxy.contains(":")){
	    String[] arr = http_proxy.split(":");
	    if(arr.length != 2)
		throw new IOException("Cannot interpret http-proxy in conf");
	    System.setProperty("https.proxyHost", arr[0]);
	    System.setProperty("https.proxyPort", arr[1]);
	}

    }
}
