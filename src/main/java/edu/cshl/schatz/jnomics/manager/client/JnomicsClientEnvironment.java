package edu.cshl.schatz.jnomics.manager.client;

import java.io.File;

/**
 * User: james
 */
public class JnomicsClientEnvironment {

    public static File USER_CONF_DIR = new File(System.getProperty("user.home"),".jnomics");
    public static File USER_AUTH_FILE = new File(USER_CONF_DIR,"globus_auth.properties");
}
