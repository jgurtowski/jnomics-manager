/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: Sun Sep 18 14:06:40 EDT 2011 (revision 812)
 */

package edu.cshl.schatz.jnomics.attic;

import java.io.IOException;
import java.io.InputStream;
import java.util.ListIterator;

import edu.cshl.schatz.jnomics.io.DNASequenceReader;
import edu.cshl.schatz.jnomics.ob.AbstractNucleotideSequence;
import edu.cshl.schatz.jnomics.ob.Base;
import edu.cshl.schatz.jnomics.ob.DNASequence;
import edu.cshl.schatz.jnomics.ob.SequenceSegment;
import edu.cshl.schatz.jnomics.ob.writable.NucleotideSequenceWritable;

/**
 * <p>
 * This is an efficient {@link InputStream} implementation that allows sequence
 * data contained {@link DNASequence} and {@link SequenceSegment} objects to be
 * conveniently read into a <code>byte[]</code> array. If a {@link DNASequence}
 * is being read that contains multiple {@link SequenceSegment} elements, their
 * contents are output seamlessly as a single stream.
 * </p>
 * <p>
 * The byte values returned are upper-case ASCII characters that represent an
 * IUPAC code. They can be cast to <code>character</code> values with no
 * overhead, or can be passed to the <code>characterToBase(int)</code> in the
 * {@link Base} class.
 * </p>
 * <p>
 * If the developer's goal is simply to retrieve the sequence and convert it to
 * a String, it may be more convenient to use the {@link DNASequenceReader}
 * class.
 * </p>
 * TODO Refactor this into a more generalized "NucleotideSequenceInputStream".
 * See: {@link AbstractNucleotideSequence#read(byte[])}
 * 
 * @see DNASequenceReader
 * @deprecated Use {@link NucleotideSequenceWritable} instead.
 * @author Matthew Titmus
 */
@Deprecated
@SuppressWarnings("unused")
public class DNASequenceInputStream extends InputStream {
    private SequenceSegment currentSegment = null;
    private ListIterator<SequenceSegment> iterator;
    private int readlimit = 0;
    /**
     * [0] = range index; [1] = base index in the {@link SequenceSegment} at
     * index [0].
     */
    private int[] readPointer, mark;

    private DNASequence sequence;

    /**
     * Creates a <code>DNASequenceInputStream</code> instance that will read the
     * contents of <code>sequence</code>.
     */
    public DNASequenceInputStream(DNASequence sequence) {
        this.sequence = sequence;

        init();
    }

    /**
     * Creates a <code>DNASequenceInputStream</code> instance that will read the
     * contents of <code>segment</code>.
     */
    public DNASequenceInputStream(SequenceSegment segment) {
        sequence = new DNASequence("", "");

        /*
         * TODO FIX ME sequence.segments.add(segment);
         */

        init();
    }

    /*
     * @see java.io.InputStream#available()
     */
    @Override
    public int available() {
        if ((currentSegment == null) || (currentSegment.length() <= readPointer[1])) {

            return 0;
        } else {
            return currentSegment.length() - readPointer[1];
        }
    }

    /**
     * <p>
     * Marks the current position in this input stream. A subsequent call to the
     * reset method repositions this stream at the last marked position so that
     * subsequent reads re-read the same bytes.
     * <p>
     * The readlimit arguments tells this input stream to allow that many bytes
     * to be read before the mark position gets invalidated.
     * <p>
     * The general contract of mark is that, if the method markSupported returns
     * true, the stream somehow remembers all the bytes read after the call to
     * mark and stands ready to supply those same bytes again if and whenever
     * the method reset is called. However, the stream is not required to
     * remember any data at all if more than readlimit bytes are read from the
     * stream before reset is called.
     * <p>
     * Marking a closed stream should not have any effect on the stream.
     * 
     * @param readlimit the maximum limit of bytes that can be read before the
     *            mark position becomes invalid.
     * @see java.io.InputStream#mark(int)
     */
    @Override
    public synchronized void mark(int readlimit) {
        mark = new int[] { readPointer[0], readPointer[1] };
    }

    /*
     * @see java.io.InputStream#markSupported()
     */
    @Override
    public boolean markSupported() {
        return true;
    }

    /**
     * Reads the next base from the DNASequence. Repeatedly calling this method
     * is far, far less efficient than using <code>read(byte[], int, int)</code>
     * .
     * 
     * @see java.io.InputStream#read()
     */
    @Override
    public int read() throws IOException {
        byte[] b = new byte[1];

        read(b);

        return b[0];
    }

    /**
     * <p>
     * Calls <code>read(byte[] b, int off, int len)</code> passing
     * <code>0</code> to <code>off</code> and <code>b.length</code> as
     * <code>len</code>. An attempt is made to read as many as
     * <code>b.length</code> bytes, but a smaller number may be read. The number
     * of bytes actually read is returned as an integer.
     * </p>
     * <p>
     * If no byte is available because the stream is at the end, the value
     * <code>-1</code> is returned; otherwise, at least one byte is read and
     * stored into <code>b</code>.
     * <p>
     * 
     * @param b the buffer into which the data is read.
     * @return the total number of bytes read into the buffer, or -1 if there is
     *         no more data because the end of the stream has been reached.
     * @throws IOException If the underlying {@link DNASequence} has been
     *             modified since the previous <code>read</code> call
     * @throws NullPointerException If <code>b</code> is <code>null</code>.
     * @throws IndexOutOfBoundsException If <code>off</code> is negative,
     *             <code>len</code> is negative, or <code>len</code> is greater
     *             than <code>b.length - off</code>
     * @see java.io.InputStream#read(byte[])
     */
    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    /**
     * <p>
     * Reads up to <code>len</code> bytes of data from the input stream into an
     * array of bytes. An attempt is made to read as many as <code>len</code>
     * bytes, but a smaller number may be read. The number of bytes actually
     * read is returned as an integer.
     * <p>
     * If <code>len</code> is zero, then no bytes are read and <code>0</code> is
     * returned; otherwise, there is an attempt to read at least one byte. If no
     * byte is available because the stream is at end of file, the value
     * <code>-1</code> is returned; otherwise, at least one byte is read and
     * stored into <code>b</code>.
     * <p>
     * The first byte read is stored into element <code>b[off]</code>, the next
     * one into <code>b[off+1]</code>, and so on. The number of bytes read is,
     * at most, equal to <code>len</code>. Let <code>k</code> be the number of
     * bytes actually read; these bytes will be stored in elements
     * <code>b[off]</code> through <code>b[off+k-1]</code>, leaving elements
     * <code>b[off+k]</code> through <code>b[off+len-1]</code> unaffected.
     * <p>
     * In every case, elements <code>b[0]</code> through <code>b[off]</code> and
     * elements <code>b[off+len]</code> through <code>b[b.length-1]</code> are
     * unaffected.
     * 
     * @param b the buffer into which the data is read.
     * @param off the start offset in array b at which the data is written.
     * @param len the maximum number of bytes to read.
     * @return the total number of bytes read into the buffer, or -1 if there is
     *         no more data because the end of the stream has been reached.
     * @throws IOException If the underlying {@link DNASequence} has been
     *             modified since the previous <code>read</code> call
     * @throws NullPointerException If <code>b</code> is <code>null</code>.
     * @throws IndexOutOfBoundsException If <code>off</code> is negative,
     *             <code>len</code> is negative, or <code>len</code> is greater
     *             than <code>b.length - off</code>
     * @see java.io.InputStream#read(byte[], int, int)
     */
    @Override
    public int read(byte[] b, int off, int len) {
        int total = 0, read = Integer.MAX_VALUE;
        SequenceSegment seg;

        while ((total < len) && (read > 0) && (null != (seg = getSegment()))) {
            read = seg.read(b, readPointer[1], total, len - total);
            total += read;
            readPointer[1] += read;
        }

        if (0 > (readlimit -= total)) {
            mark = null;
        }

        // If we've reached the end, return -1.
        if (total == 0) {
            total = -1;
        }

        return total;
    }

    /*
     * @see java.io.InputStream#reset()
     */
    @Override
    public synchronized void reset() throws IOException {
        readPointer = mark;
        /*
         * TODO FIX ME iterator = sequence.segments.listIterator(mark[0]);
         */
        readlimit = 0;
        currentSegment = null;
    }

    /*
     * @see java.io.InputStream#skip(long)
     */
    @Override
    public long skip(long n) throws IOException {
        byte[] b = new byte[1024];
        int c = (int) n, read;

        while (-1 < (read = read(b, 0, c > b.length ? b.length : c))) {
            if (0 <= (n -= read)) {
                break;
            }
        }

        return n - c;
    }

    /**
     * Returns the next {@link SequenceSegment}, or <code>null</code> if no more
     * are available.
     */
    private SequenceSegment getSegment() {
        if ((currentSegment == null) || (available() == 0)) {
            readPointer[0] = iterator.nextIndex();
            readPointer[1] = 0;
            currentSegment = iterator.hasNext() ? iterator.next() : null;
        }

        return currentSegment;
    }

    private void init() {
        /*
         * TODO FIX ME iterator = sequence.segments.listIterator();
         */
        readPointer = new int[] { 0, 0 };
        mark = null;
        readlimit = 0;
        currentSegment = null;
    }
}
