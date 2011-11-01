/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: Sun Sep 18 14:06:40 EDT 2011 (revision 812)
 */

package edu.cshl.schatz.jnomics.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.UnsupportedEncodingException;

import edu.cshl.schatz.jnomics.attic.DNASequenceInputStream;
import edu.cshl.schatz.jnomics.ob.DNASequence;
import edu.cshl.schatz.jnomics.ob.SequenceSegment;

/**
 * <p>
 * This is an efficient {@link Reader} implementation that allows sequence data
 * contained {@link DNASequence} and {@link SequenceSegment} objects to be
 * conveniently retrieved as a String via the <code>readLine()</code> method in
 * a method analogous to the method of the same name in the
 * {@link BufferedReader} class. Behind the scenes, this uses a
 * {@link DNASequenceInputStream}, and will thus seamlessly return the contents
 * of multiple {@link SequenceSegment} elements of a {@link DNASequence} as a
 * single sequence.
 * </p>
 * 
 * @see DNASequenceInputStream
 * @author Matthew Titmus
 */
@SuppressWarnings("deprecation")
public class DNASequenceReader extends Reader {
    /**
     * The default number of characters returned in a call to
     * <code>readLine()</code>.
     */
    public static final int DEFAULT_LINE_WIDTH = 80;

    private byte[] buffer;

    private DNASequenceInputStream istream;

    /**
     * @param sequence The {@link DNASequence} to read.
     */
    public DNASequenceReader(DNASequence sequence) {
        this(sequence, DEFAULT_LINE_WIDTH);
    }

    /**
     * @param sequence The {@link DNASequence} to read.
     * @param lineWidth The number of characters to be returned by
     *            <code>readLine()</code>.
     */
    public DNASequenceReader(DNASequence sequence, int lineWidth) {
        buffer = new byte[lineWidth];
        istream = new DNASequenceInputStream(sequence);
    }

    /*
     * @see java.io.Reader#close()
     */
    @Override
    public void close() throws IOException {
        istream.close();
    }

    /*
     * @see java.io.Reader#read(char[], int, int)
     */
    @Override
    public int read(char[] cbuf, int off, int len) {
        if (buffer.length < cbuf.length) {
            buffer = new byte[cbuf.length];
        }

        int read = istream.read(buffer, off, len);
        for (int i = 0; i < cbuf.length; i++) {
            cbuf[i] = (char) buffer[i];
        }

        return read;
    }

    public String readLine() {
        int read = istream.read(buffer, 0, buffer.length);
        String str = null;

        try {
            str = read >= 0 ? new String(buffer, 0, read, "UTF-8") : null;
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        return str;
    }
}
