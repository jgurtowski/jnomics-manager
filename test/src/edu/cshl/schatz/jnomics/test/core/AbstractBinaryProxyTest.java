/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.test.core;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;
import junit.framework.TestCase;
import edu.cshl.schatz.jnomics.mapreduce.DistributedBinary;

/**
 * @author Matthew Titmus
 */
public class AbstractBinaryProxyTest extends TestCase {
    File localFile;
    String localFileName, nonExistantName;

    public void testFindBinaryDefaultPath() throws IOException {
        Assert.assertNotNull(doFindBinary("./" + localFileName));
        Assert.assertNotNull(doFindBinary("mv"));
        Assert.assertNull(doFindBinary("./" + nonExistantName));
        Assert.assertNull(doFindBinary(nonExistantName));
    }

    public void testFindBinarySpecificPath() throws IOException {
        String path;

        path = new File("/").getCanonicalPath();
        path += DistributedBinary.PATH_SEPARATOR;
        path = new File("/bin").getCanonicalPath();
        path += DistributedBinary.PATH_SEPARATOR;
        path += new File(".").getCanonicalPath();

        Assert.assertNotNull(doFindBinary(localFileName, path));
    }

    /*
     * @see junit.framework.TestCase#setUp()
     */
    @Override
    protected void setUp() throws Exception {
        localFileName = Long.toHexString(System.currentTimeMillis()) + ".tmp";
        nonExistantName = "jdg24yj98sposjhvFGRT54Ahfjkfggukyrtyjf";

        localFile = new File(localFileName);

        localFile.createNewFile();
    }

    /*
     * @see junit.framework.TestCase#tearDown()
     */
    @Override
    protected void tearDown() throws Exception {
        localFile.delete();
    }

    private File doFindBinary(String command) throws IOException {
        File binary = DistributedBinary.findFile(command);
        String binaryName = null;

        if (null != (binary = DistributedBinary.findFile(command))) {
            binaryName = binary.getCanonicalPath();
        }

        System.out.printf("Command: \"%s\"; File: %s%n", command, binaryName);

        return binary;
    }

    private File doFindBinary(String command, String path) throws IOException {
        File binary = DistributedBinary.findFile(command);
        String binaryName = null;

        if (null != (binary = DistributedBinary.findFile(command, path))) {
            binaryName = binary.getCanonicalPath();
        }

        System.out.printf("Command: \"%s\"; File: %s%n", command, binaryName);

        return binary;
    }
}
