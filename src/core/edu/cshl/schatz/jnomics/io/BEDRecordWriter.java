/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.io;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import edu.cshl.schatz.jnomics.mapreduce.JnomicsRecordWriter;
import edu.cshl.schatz.jnomics.ob.Orientation;
import edu.cshl.schatz.jnomics.ob.writable.SequencingRead;

/**
 * A record writer that generates BED6-formatted files (see The BEDTools manual,
 * http://code.google.com/p/bedtools/)
 * 
 * @author Matthew A. Titmus
 */
public class BEDRecordWriter<K, V> extends JnomicsRecordWriter<K, V> {
    /**
     * The read property (equivalent to a [BS]AM "tag") to use in the BED
     * "score" field. By default, this is the SAM 'NM:i' tag (an integer equal
     * to the edit distance to the reference, including ambiguous bases but
     * excluding clipping). If this property in undefined in the read, the BED
     * score will be set to <code>0</code> and a warning will be logged.
     */
    public static final String P_SCORE_PROPERTY = "jnomics.bed.score.property";

    private static final Log LOG = LogFactory.getLog(BEDRecordWriter.class);

    /**
     * The default read property to use in the BED "score" field.
     */
    private static final Text DEFAULT_SCORE_PROPERTY = new Text("NM:i");

    private static final Text DEFAULT_SCORE_VALUE = new Text("0");

    /** TODO: Make this settable via a property of some kind. */
    private Text scoreTag = DEFAULT_SCORE_PROPERTY;

    /**
     * @param out The {@link DataOutputStream} to write to.
     * @param config The {@link Configuration} of the user class.
     */
    public BEDRecordWriter(DataOutputStream out, Configuration config) {
        super(out, config);
    }

    /**
     * {@inheritDoc}
     * 
     * @see JnomicsRecordWriter#writeSequencingRead(SequencingRead)
     */
    @Override
    protected void writeSequencingRead(SequencingRead read) throws IOException {
        Writable scoreColumn;
        int first = read.getEndpoints().first() - 1, last;

        if (!read.isMapped()) {
            return;
        }

        if (0 != (last = cigarToLength(read.getCigar()))) {
            last = first + last;
        } else {
            last = read.getEndpoints().last();
        }

        // 1. chrom - The name of the chromosome on which the genome feature
        // exists (required).
        writeObject(read.getReferenceNameText());
        writeTab();

        // 2. start - The zero-based starting position of the feature in the
        // chromosome (required).
        writeObject(first);
        writeTab();

        // 3. end - The one-based ending position of the feature in the
        // chromosome (required).
        writeObject(last);
        writeTab();

        // 4. name - Defines the name of the BED feature (optional).
        writeObject(read.getReadName());
        writeTab();

        // 5. score – The UCSC definition requires that a BED score range
        // from 0 to 1000, inclusive (optional).
        if (null == (scoreColumn = read.getProperties().get(scoreTag))) {
            LOG.warn("Tag " + scoreTag + " not defined for read " + read.getReadName());
            scoreColumn = DEFAULT_SCORE_VALUE;
        }
        writeObject(scoreColumn);
        writeTab();

        // 6. strand - Defines the strand - either '+' or '-' (optional).
        if (read.getOrientation() != Orientation.UNSPECIFIED) {
            out.write((byte) (read.getOrientation() == Orientation.PLUS ? '+' : '-'));
        }

        writeNewline();
    }

    /**
     * Uses a CIGAR-formatted {@link Text} value to compute the alignment end
     * coordinate in an "ungapped" fashion. That is, match ("M"), deletion
     * ("D"), and splice ("N") operations are observed when computing alignment
     * ends.
     * <p>
     * This functionality was duplicated from the "bamToBed" component of the
     * BEDtools toolkit (<a href="http://code.google.com/p/bedtools"/>), created
     * by Aaron R. Quinlan and Ira M. Hall at the University of Virginia.
     */
    public static int cigarToLength(Text cigar) {
        final int ASCII_0 = 0x30;
        final int ASCII_9 = 0x39;
        final int ASCII_M = 0x4D;
        final int ASCII_N = 0x4E;
        final int ASCII_D = 0x44;

        byte[] bb = cigar.getBytes();
        int len = cigar.getLength();

        int value = 0, sum = 0;
        for (int i = 0; i < len; i++) {
            int c = bb[i];

            if (c >= ASCII_0 && c <= ASCII_9) {
                value *= 10;
                value += (c - ASCII_0);
            } else {
                switch (c) {
                case ASCII_D:
                case ASCII_M:
                case ASCII_N:
                    sum += value;
                    break;
                }

                value = 0;
            }
        }

        return sum;
    }
}
