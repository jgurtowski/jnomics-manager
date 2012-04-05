/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.test.core;

import org.apache.hadoop.fs.Path;

import edu.cshl.schatz.jnomics.mapreduce.ReadFileFormat;

/**
 * @author Matthew Titmus
 */
public class BAMSequencingReadInputFormatTest extends AbstractReadInputFormatTest {
    private final ReadFileFormat expectedReadFileFormat = ReadFileFormat.BAM;

    private final Path[] inPaths = { new Path(FULL_FILE_PATH + "example.bam") };

    private final Path[] singleLineFileInPaths = { new Path(SINGLE_LINE_FILE_PATH + "example.bam") };

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
        return inPaths;
    }

    @Override
    public Path[] getSingleLineFilePaths() {
        return singleLineFileInPaths;
    }
}
