/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.test.core;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import edu.cshl.schatz.jnomics.io.FastqRecordWriter;
import edu.cshl.schatz.jnomics.io.JnomicsFileRecordReader;
import edu.cshl.schatz.jnomics.io.SequencingReadInputFormat;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsRecordWriter;
import edu.cshl.schatz.jnomics.mapreduce.ReadFileFormat;
import edu.cshl.schatz.jnomics.ob.writable.QueryTemplate;

/**
 * @author Matthew A. Titmus
 */
public class FastqSequencingRecordReaderTest extends AbstractReadInputFormatTest {
    private final ReadFileFormat expectedReadFileFormat = ReadFileFormat.FASTQ;

    private static final String[] IN_FILES = { "example.1.fq", "example.2.fq" };

    private final Path[] fullFileInPaths = {
            new Path(FULL_FILE_PATH + IN_FILES[0]),
            new Path(FULL_FILE_PATH + IN_FILES[1]) };

    private final Path[] singleLineFileInPaths = {
            new Path(SINGLE_LINE_FILE_PATH + IN_FILES[0]),
            new Path(SINGLE_LINE_FILE_PATH + IN_FILES[1]) };

    public static void main(String[] args) throws Exception {
        FastqSequencingRecordReaderTest t = new FastqSequencingRecordReaderTest();

        t.testWriteFormat();
    }

    /*
     * @see edu.cshl.schatz.jnomics.test.core.AbstractReadInputFormatTest#
     * getExpectedReadFileFormat()
     */
    @Override
    public ReadFileFormat getExpectedReadFileFormat() {
        return expectedReadFileFormat;
    }

    /*
     * @see
     * edu.cshl.schatz.jnomics.test.core.AbstractReadInputFormatTest#getInPaths
     * ()
     */
    @Override
    public Path[] getFullFileInPaths() {
        return fullFileInPaths;
    }

    /*
     * @see edu.cshl.schatz.jnomics.test.core.AbstractReadInputFormatTest#
     * getSingleLineFilePaths()
     */
    @Override
    public Path[] getSingleLineFilePaths() {
        return singleLineFileInPaths;
    }

    public void testWriteFormat() throws IOException, InterruptedException {
        final String GEN_DIR = "/tmp/inputFormats/"; 
        final String STANDARD_SAM = FULL_FILE_PATH + "example.sam";
        final String GENERATED_FQ[] = { "test.1.fq", "test.2.fq" };
        final String STANDARD_FQ[] = {
            FULL_FILE_PATH + "example.1.fq",
            FULL_FILE_PATH + "example.2.fq" };

        new File(GEN_DIR).mkdirs();        
        File[] generatedFastq = { new File(GEN_DIR, GENERATED_FQ[0]), new File(GEN_DIR, GENERATED_FQ[1]) };
        File[] expectedFastq = { new File(STANDARD_FQ[0]), new File(STANDARD_FQ[1]) };
        
        Configuration conf = new Configuration();
        QueryTemplate standard = new QueryTemplate();
        JnomicsFileRecordReader samStandard;
        JnomicsRecordWriter<Text, QueryTemplate> writer1, writer2;

        // Read query templates from the standard SAM file and convert them
        // into paired fastq files.

        samStandard = SequencingReadInputFormat.newRecordReader(ReadFileFormat.SAM);
        samStandard.initialize(new Path(STANDARD_SAM), conf);

        writer1 = new FastqRecordWriter<Text, QueryTemplate>(
            new DataOutputStream(new FileOutputStream(generatedFastq[0])), conf,
            FastqRecordWriter.MEMBER_FIRST);

        writer2 = new FastqRecordWriter<Text, QueryTemplate>(
            new DataOutputStream(new FileOutputStream(generatedFastq[1])), conf,
            FastqRecordWriter.MEMBER_LAST);

        while (samStandard.nextKeyValue()) {
            standard.set(samStandard.getCurrentValue());
            writer1.write(standard.getTemplateName(), standard);
            writer2.write(standard.getTemplateName(), standard);
        }

        String[] expected = new String[4], actual = new String[4];
        // StringBuilder builder;
        BufferedReader ain, ein;
        int lineNumber = 1;

        for (int idx = 0; idx < 2; idx++, lineNumber = 1) {
            ain = new BufferedReader(new FileReader(generatedFastq[idx]));
            ein = new BufferedReader(new FileReader(expectedFastq[idx]));

            do {
                String msg = "Line " + lineNumber + ": ";

                for (int c = 0; c < 4; c++) {
                    actual[c] = ain.readLine();
                    expected[c] = ein.readLine();
                }

                for (int c = 0; c < 4; c++) {
                    assertEquals(
                        msg + " Expected=" + expected[c] + "; Actual=" + actual[c],
                        expected[c],
                        actual[c]);
                }

                lineNumber += 4;
            } while ((actual[3] != null) && (expected[3] != null));
        }
    }
}
