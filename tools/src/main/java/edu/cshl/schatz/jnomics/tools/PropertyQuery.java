/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.tools;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

/**
 * Used to query default Hadoop job properties without needing to locate and
 * parse any configuration files.
 * 
 * @author Matthew Titmus
 */
public class PropertyQuery {
    public static final String CMD_DESCRIPTION = "Query Hadoop MapReduce job properties.";

    public static final String CMD_NAME = "query";

    /**
     * Dumps all of the properties set in a {@link Configuration} instance to
     * the standard output stream.
     * 
     * @param conf An instance of {@link Configuration}. If <code>null</code>, a
     *            new instance will be created and filled with the defaults for
     *            the Hadoop libraries in the running JVM's classpath.
     * @param optionsList Zero or more {@link QueryOptions}.
     * @throws IOException
     */
    public static void dumpProperties(Configuration conf, QueryOptions... optionsList)
            throws IOException {

        Map<String, String> unifiedProperties = new HashMap<String, String>(307);

        // Put the options in a set so they're easier to find
        HashSet<QueryOptions> options = new HashSet<PropertyQuery.QueryOptions>();
        Collections.addAll(options, optionsList);

        if (conf == null) {
            conf = new Job().getConfiguration();
        }

        // Grab the properties in the Configuration instance.
        for (Entry<String, String> e : conf) {
            unifiedProperties.put(e.getKey(), query(conf, e.getKey(), optionsList));
        }

        // If the user wants the system props too, throw those in.
        if (options.contains(QueryOptions.INCLUDE_VM)) {
            for (Entry<Object, Object> e : System.getProperties().entrySet()) {
                String k = e.getKey().toString();
                String v = e.getValue().toString();

                unifiedProperties.put(k, v);
            }
        }

        // If the user wants the system props too, throw those in.
        if (options.contains(QueryOptions.INCLUDE_ENVIRONMENT)) {
            for (Entry<String, String> e : System.getenv().entrySet()) {
                String k = e.getKey();
                String v = e.getValue();

                unifiedProperties.put(k, v);
            }
        }

        // Throw all the keys into an array for sorting. Alphabetical order is
        // SO much easier to read.

        String[] keys = new String[unifiedProperties.size()];
        int i = 0;

        for (Entry<String, String> e : unifiedProperties.entrySet()) {
            keys[i++] = e.getKey();
        }

        Arrays.sort(keys);

        // Shake and pour.

        for (String key : keys) {
            System.out.printf("%s=%s%n", key, unifiedProperties.get(key));
        }
    }

    /**
     * TODO Add support for standard Hadoop CLI options.
     * 
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        int exit = 1;

        if (args.length != 1) {
            System.err.printf("Syntax: %s <all|property>%n", CMD_NAME);
            System.exit(exit);
        } else {
            Configuration conf = new Job().getConfiguration();

            if (args[0].equals("config")) {
                dumpProperties(conf);
            } else if (args[0].equals("vm")) {
                dumpProperties(conf, QueryOptions.INCLUDE_VM);
            } else if (args[0].equals("env")) {
                dumpProperties(conf, QueryOptions.INCLUDE_VM);
            } else if (args[0].equals("all")) {
                dumpProperties(conf, QueryOptions.INCLUDE_VM, QueryOptions.INCLUDE_ENVIRONMENT);
            } else {
                System.out.println(conf.get(args[0], "(null)"));
            }
        }
    }

    /**
     * Dumps all of the properties set in a {@link Configuration} instance to
     * the standard output stream.
     * 
     * @param conf An instance of {@link Configuration}. If <code>null</code>, a
     *            new instance will be created and filled with the defaults for
     *            the Hadoop libraries in the running JVM's classpath.
     * @param optionsList Zero or more {@link QueryOptions}.
     */
    public static String query(Configuration conf, String name, QueryOptions... optionsList) {

        // Put the options in a set so they're easier to find
        HashSet<QueryOptions> options = new HashSet<PropertyQuery.QueryOptions>();
        Collections.addAll(options, optionsList);
        String value;

        boolean raw = options.contains(QueryOptions.RAW_VALUES);

        if (conf == null) {
            conf = new Configuration();
        }

        if ((null == (value = raw ? conf.getRaw(name) : conf.get(name)))
                && options.contains(QueryOptions.INCLUDE_VM)) {

            value = System.getProperty(name);
        }

        return value;
    }

    public static enum QueryOptions {
        /**
         * Include the system properties (via
         * <code>System.getProperties()</code>). Default is <code>false</code>.
         */
        INCLUDE_ENVIRONMENT,
        /**
         * Include the system properties (via
         * <code>System.getProperties()</code>). Default is <code>false</code>.
         */
        INCLUDE_VM,

        /**
         * Get the value of the property, without doing any variable expansion.
         */
        RAW_VALUES
    }
}
