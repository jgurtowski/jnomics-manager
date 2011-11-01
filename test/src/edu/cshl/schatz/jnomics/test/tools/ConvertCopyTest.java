/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.test.tools;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;

import edu.cshl.schatz.jnomics.tools.ConvertCopy;
import edu.cshl.schatz.jnomics.tools.PropertyQuery;

/**
 * @author Matthew Titmus
 */
public class ConvertCopyTest extends TestCase {
    static final String FS_DEFAULT_NAME = "fs.default.name";

    public void testInterpretFileURI() throws IOException, URISyntaxException {
        Configuration conf = new Configuration();

        final String FILE = "foo.bar";
        final String FS = "file:/";
        File f = new File(FILE);

        // File schema, relative URI
        doTestInterpretURI(FILE, "file:" + f.getCanonicalPath(), conf);

        // File schema, absolute URI
        doTestInterpretURI("/" + FILE, FS + FILE, conf);

        // Wild-card HDFS schema into file schema, absolute URI
        doTestInterpretURI("hdfs://?/" + FILE, FS + FILE, conf);

        // Wild-card HDFS schema into file schema, absolute URI
        doTestInterpretURI("hdfs://?//" + FILE, FS + FILE, conf);
    }

    public void testInterpretHdfsURI() throws IOException, URISyntaxException {
        Configuration conf = new Configuration();

        final String FILE = "foo.bar";

        // HDFS w/host, relative URI
        String defaultFS = PropertyQuery.query(conf, ConvertCopyTest.FS_DEFAULT_NAME);
        String userDir = System.getProperty("user.dir");
        String expected;

        // Standardize... ":///" == ":/"
        if (defaultFS.matches("^[^/]+:(//+)/$")) {
            defaultFS = defaultFS.replaceAll(":(//+)/$", ":/");
        }

        doTestInterpretURI(FILE, "file:" + userDir + "/" + FILE, conf);

        // HDFS w/host, absolute URI
        expected = (defaultFS + FILE);
        doTestInterpretURI("/" + FILE, expected, conf);

        // Wild-card HDFS schema into qualified HDFS schema, absolute URI
        doTestInterpretURI("hdfs://?/" + FILE, defaultFS + FILE, conf);

        // Wild-card HDFS schema into qualified HDFS schema, absolute URI
        doTestInterpretURI("hdfs://?//" + FILE, defaultFS + FILE, conf);
    }

    public void testSetDefaultFS() throws IOException, URISyntaxException {
        String init;

        Configuration conf = new Configuration();
        assertNotNull(init = conf.get(ConvertCopyTest.FS_DEFAULT_NAME));

        conf.set(ConvertCopyTest.FS_DEFAULT_NAME, "hdfs://localhost:9000/");
        assertNotSame(init, conf.get(ConvertCopyTest.FS_DEFAULT_NAME));
    }

    private void doTestInterpretURI(String uri, String expect, Configuration conf)
            throws IOException, URISyntaxException {

        if (conf == null) {
            conf = new Configuration();
        }

        String result = ConvertCopy.interpretURI(uri, conf);

        System.out.printf("Arg=%s Expected=%s; Result=%s%n", uri, expect, result);

        assertEquals(expect, result);
    }
}
