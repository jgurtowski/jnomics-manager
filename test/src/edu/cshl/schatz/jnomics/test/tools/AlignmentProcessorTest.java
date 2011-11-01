/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.test.tools;

import java.util.regex.Matcher;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.util.ToolRunner;

import edu.cshl.schatz.jnomics.mapreduce.JnomicsTool;
import edu.cshl.schatz.jnomics.tools.AlignmentProcessor;

/**
 * @author Matthew Titmus
 */
public class AlignmentProcessorTest extends TestCase {
    public static final String DIR_TEST_GENOMES = "test-data/fasta";

    public static final String DIR_TEST_ROOT = "test-data";
    public static final String TEST_REF_GENOME = DIR_TEST_GENOMES + "/EcoliK12.fa";
    public static final String TEST_SV_GENOME = DIR_TEST_GENOMES + "/EcoliK12.sv.fa";

    public static void main(String[] args) throws Exception {
        int status;

        StringBuilder command = new StringBuilder();

        command.append(" -f " + TEST_REF_GENOME);
        command.append(" -fq /home/mtitmus/Dev/data.genomes/EcoliK12/EcoliK12.fa.sim.1.fq /home/mtitmus/Dev/data.genomes/EcoliK12/EcoliK12.fa.sim.2.fq");
        command.append(" -t 1 --scripts /home/mtitmus/build/jnomics/core/scripts");
        command.append(" -dir build");

        // $HADOOP_DIR/hadoop jar $JAR_FILE bwa $HADOOP_CONF $HADOOP_OPTIONS \
        // -in $FS_DIR/tier1_1 -out $FS_DIR/tier1_2 \
        // -fout sam \
        // -db $FASTA \
        // -D mapred.job.name="hydra.tier1.bwa.alignment" \
        // -D jnomics.map.fail.no-reads.in="false" \
        // -D mapred.reduce.tasks=$HADOOP_NODE_COUNT \
        // -D mapred.task.timeout=$TIMEOUT_LONG

        JnomicsTool tool = new AlignmentProcessor();
        status = ToolRunner.run(tool, command.toString().split(" "));
        assertEquals(
            "AlignmentProcessor.run(String[]) exited with a status code of " + status, status);
    }

    public void testPathSchemaExistsPattern() {
        // Paths with a schema; should match the pattern
        String[] expectTrue = {
                "file:/home/me",
                "file://home/me",
                "hdfs://home/me",
                "hdfs://localhost:9000/home/me", };

        // Paths with no schema; shouldn't match the pattern
        String[] expectFalse = { "/home/me" };

        Matcher matcher;

        for (String path : expectFalse) {
            matcher = JnomicsTool.SCHEMA_EXISTS_PATTERN.matcher(path);

            Assert.assertFalse(matcher.matches());
        }

        for (String path : expectTrue) {
            matcher = JnomicsTool.SCHEMA_EXISTS_PATTERN.matcher(path);

            Assert.assertTrue(matcher.matches());
        }
    }
}
