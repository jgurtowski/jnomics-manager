/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.test.tools;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import junit.framework.TestCase;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsTool;
import edu.cshl.schatz.jnomics.tools.DistributedBWA;

/**
 * @author Matthew A. Titmus
 */
public class DistributedBWATest extends TestCase {
    private static final String ARG_DB = "/bluearc/data/schatz/data/genomes/hg19/hg19.fa";

    private static final String ARG_IN = "example-data/inputFormats/example.flq";

    private static final String ARG_OUT = "/tmp/bwaTest";

    /**
     * No args: The process should return 0.
     */
    public void testNoArgs() throws Exception {
        DistributedBWA bin = new DistributedBWA();

        // Does the process exit successfully?
        assertEquals(JnomicsTool.STATUS_OK, JnomicsTool.run(bin, new String[] {}));

        // "group.name" gets set by the Hadoop framework when a distributed job
        // is executed, so it should not be null here.
        assertNull(
            "The job was executed when there were no input parameters.",
            bin.getConf().get("group.name"));
    }

    /**
     * Runs a small end-to-end {@link DistributedBWA} process.
     */
    public void testRunBwa() throws Exception {
        String args = "";

        args += " -db " + ARG_DB;
        args += " -in " + ARG_IN;
        args += " -out " + ARG_OUT;
        args += " -D mapred.job.name=DistributedBWATest";

        DistributedBWA bin = new DistributedBWA();

        FileSystem fs = FileSystem.getLocal(bin.getConf());
        Path out = new Path(ARG_OUT);
        if (fs.exists(out)) {
            fs.delete(out, true);
        }

        // Does the process exit successfully?
        assertEquals(0, JnomicsTool.run(bin, args.split(" ")));

        // "group.name" gets set by the Hadoop framework when a distributed job
        // is executed, so it should not be null here.
        assertNotNull(
            "Connfiguration property \"group.name\" was undefined: the job must not have run.",
            bin.getConf().get("group.name"));
    }
}
