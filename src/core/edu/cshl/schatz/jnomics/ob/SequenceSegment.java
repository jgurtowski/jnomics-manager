/*
 * Copyright (C) 2011 Matthew A. Titmus
 */

package edu.cshl.schatz.jnomics.ob;

import static edu.cshl.schatz.jnomics.ob.Orientation.MINUS;
import static edu.cshl.schatz.jnomics.ob.Orientation.PLUS;

import java.nio.charset.Charset;

/**
 * This class represents a contiguous sequence of nucleotides (a "segment")
 * within a larger DNA sequence (specifically, a {@link DNASequence} instance
 * contains one or more ordered {@link SequenceSegment} objects). In this
 * context, regions of the containing sequence that lie next to one another, but
 * whose original positions and/or orientations were not contiguous, would be
 * represented by separate {@link SequenceSegment} instances.
 * 
 * @author Matthew Titmus
 */
public class SequenceSegment extends AbstractNucleotideSequence implements NucleotideSequence {
    private static final String STRING_FORMAT = //
    "%1$d\t" + // 5' Position of 5'-most base in current sequence
            "%2$d\t" + // Number of bases
            "%3$s\t" + // Sequence source ID, if known.
            "%4$s\t" + // Position of 5' base in original sequence
            "%5$s\t" + // Sequence orientation relative to its source
            "%6$s"; // Sequence contents, if source ID is unknown.

    private byte[] rawBytes = new byte[0];

    private String referenceName = "";

    private PositionRange referencePosition = new PositionRange(0, 0);

    /**
     * Creates a new empty SequenceSegment.
     */
    public SequenceSegment() {
        super();
    }

    /**
     * Creates a new instance as a deep copy of <code>toCopy</code>.
     */
    public SequenceSegment(NucleotideSequence toCopy) {
        super();

        set(toCopy);
    }

    /**
     * Returns a tab-delimited 6- or 7-field description of this segment. The
     * contents of the fields are described below.
     * 
     * <pre>
     * Col Type Regexp   Brief description
     *  1  INT  [0-9]+   Position of the 5'-most base in the current sequence 
     *                   (first base in sequence = index 0)
     *  2  INT  [0-9]+   Number of bases contained in the segment.
     *  3  STR  [\d\s]+  The name of the segment's sequence of origin. For de
     *                   novo or other sequences of unknown origin this field
     *                   will be empty.
     *  4  INT  [0-9]+   Start position in the sequence of origin 
     *  5       [\+\-]   Orientation in relative to original sequence 
     *  6  STR  [-ACGTURYKMSWBDHVNX]   The segments nucleotide sequence; only
     *                   used when origin is unknown (field 3 is empty).
     * </pre>
     */
    public static String toString(PositionRange currentPosition, int length, String originId,
        PositionRange originPosition, Orientation orientation, String sequenceString) {

        if (originId == null) {
            originId = "";
        }

        if (sequenceString == null) {
            sequenceString = "";
        }

        return String.format(
            STRING_FORMAT, //
            currentPosition == null ? "" : currentPosition.first(), length, originId,
            originPosition == null ? "" : originPosition.first(), orientation.toString(),
            originId.equals("") ? sequenceString : "");
    }

    /**
     * Returns an array of the result of breaking this range at the specified
     * index. This object is not modified by this method.
     * 
     * @return A {@link SequenceSegment} array with a length of 1 (if the
     *         indicated breakpoint is at an existing break point) or 2 (the
     *         resulting halves of the break).
     * @throws OutOfRangeException if the requested break index is beyond the
     *             bounds of this {@link SequenceSegment}'s current position.
     */
    public SequenceSegment[] breakAt(int breakBefore) {
        PositionRange currentPosition = getEndpoints();
        SequenceSegment[] parts;

        if ((breakBefore == currentPosition.first())
                || (breakBefore == (currentPosition.last() + 1))) {

            parts = new SequenceSegment[] { this };

        } else if (currentPosition.contains(breakBefore)) {
            SequenceSegment fivePrime = null, threePrime = null;
            int currentStart, originStart, partLength;

            /*
             * Create the new segment objects for the 5' and 3' halves of the
             * segment, and calculate the start position and length of each, as
             * well as the start positions of each half relative to the
             * segment's sequence of origin. When calculating the origin
             * positions we have to account for the relative orientation of the
             * segment to its sequence of origin.
             */

            byte[] bases = getBases();

            // 5' half: Start position remains the same; length is the break
            // position - first position + 1.

            partLength = breakBefore - currentPosition.first();
            currentStart = currentPosition.first();
            originStart = getOrientation() == PLUS
                    ? referencePosition.first()
                    : (referencePosition.last() - partLength) + 1;

            fivePrime = new SequenceSegment(this);
            fivePrime.setRawBytes(bases, 0, partLength);
            fivePrime.setEndpoints(PositionRange.instanceByLength(currentStart, partLength));
            fivePrime.referencePosition = PositionRange.instanceByLength(originStart, partLength);

            // 3' half: The new start position is the break position

            partLength = length() - fivePrime.length();
            currentStart = currentPosition.first() + fivePrime.length();
            originStart = getOrientation() == MINUS
                    ? referencePosition.first()
                    : referencePosition.first() + fivePrime.length();

            threePrime = new SequenceSegment(this);
            threePrime.setRawBytes(bases, fivePrime.length(), partLength);
            threePrime.setEndpoints(PositionRange.instanceByLength(currentStart, partLength));
            threePrime.referencePosition = PositionRange.instanceByLength(originStart, partLength);

            parts = new SequenceSegment[] { fivePrime, threePrime };
        } else {
            throw new OutOfRangeException();
        }

        return parts;
    }

    /*
     * @see edu.cshl.schatz.jnomics.io.AbstractNucleotideSequence#clear()
     */
    @Override
    public void clear() {
        super.clear();

        referenceName = "";
        referencePosition = PositionRange.instanceByLength(0, 0);
    }

    /**
     * Returns <code>true</code> if this <code>SequenceSegment</code> instance
     * immediately follows <code>segment</code>. Specifically, if
     * <code>this.orientation == segment.orientation</code>, and
     * <code>this.originId.equals(segment.originId)</code>, and
     * <code>currentPosition.first() == segment.currentPosition.last() + 1.</code>
     * 
     * @param segment
     */
    public boolean follows(SequenceSegment segment) {
        return ((getOrientation() == segment.getOrientation())
                && referenceName.equals(segment.referenceName) && (getEndpoints().first() == (segment.getEndpoints().last() + 1)));
    }

    /*
     * @see edu.cshl.schatz.jnomics.io.AbstractNucleotideSequence#getRawBytes()
     */
    @Override
    public byte[] getRawBytes() {
        return rawBytes;
    }

    /*
     * @see
     * edu.cshl.schatz.jnomics.io.AbstractNucleotideSequence#getReferenceName()
     */
    @Override
    public String getReferenceName() {
        return referenceName;
    }

    /**
     * @return The originPosition
     */
    public PositionRange getReferencePosition() {
        return referencePosition;
    }

    /*
     * @see
     * edu.cshl.schatz.jnomics.io.AbstractNucleotideSequence#getSequenceString()
     */
    @Override
    public String getSequenceString() {
        return new String(rawBytes, 0, length(), Charset.forName("UTF-8"));
    }

    /**
     * <p>
     * Reads up to <code>len</code> bytes of data from this range's
     * <code>bases</code> array into an array of bytes. If
     * <code>bases.length - rangeOff < len</code>, then
     * <code>bases.length - rangeOff</code> bytes are read. The number of bytes
     * actually read is returned as an integer.
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
     * @param rangeOff the start offset in the <code>bases</code> array from
     *            which the data is read.
     * @param bOff the start offset in array <code>b</code> at which the data is
     *            written.
     * @param len the maximum number of bytes to read.
     * @return the total number of bytes read into the buffer, or -1 if there is
     *         no more data because the end of the stream has been reached.
     * @throws NullPointerException If <code>b</code> is <code>null</code>.
     * @throws IndexOutOfBoundsException If <code>off</code> is negative,
     *             <code>len</code> is negative, or <code>len</code> is greater
     *             than <code>b.length - off</code>
     * @see java.io.InputStream#read(byte[], int, int)
     */
    public int read(byte[] b, int rangeOff, int bOff, int len) {
        int available;

        if (len > (available = length() - rangeOff)) {
            len = available;
        }

        System.arraycopy(getRawBytes(), rangeOff, b, bOff, len);

        return len;
    }

    /**
     * Sets this sequence to be an identical copy of the
     * <code>sourceSequence</code> parameter. The result is effectively a deep
     * clone (i.e., no references are shared between this instance and those
     * used by <code>sourceSequence</code>.
     */
    @Override
    public void set(NucleotideSequence sequence) {
        super.set(sequence);

        if (sequence instanceof SequenceSegment) {
            SequenceSegment ss = (SequenceSegment) sequence;

            referenceName = ss.referenceName;
            referencePosition.set(ss.referencePosition);
        } else {
            referenceName = "";
            referencePosition = PositionRange.instanceByLength(0, 0);
        }
    }

    /*
     * @see
     * edu.cshl.schatz.jnomics.io.AbstractNucleotideSequence#setRawBytes(byte[],
     * int, int)
     */
    @Override
    public void setRawBytes(byte[] bases, int start, int length) {
        if (rawBytes.length < length) {
            rawBytes = new byte[length + (length / 10) + 1];
        }

        System.arraycopy(bases, start, rawBytes, 0, length);
    }

    /*
     * @see
     * edu.cshl.schatz.jnomics.io.AbstractNucleotideSequence#setReferenceName
     * (java.lang.String)
     */
    @Override
    public void setReferenceName(String referenceName) {
        this.referenceName = referenceName;
    }

    /**
     * Resets the nucleotide sequence.
     * 
     * @param bases The nucleotides sequence as an array of 8-bit IUPAC
     *            character codes (case-insensitive).
     * @param validate If <code>true</code>, the contents of bases
     *            <code>bases</code> is validated; otherwise the object's bases
     *            value is simply replaced.
     * @throws IUPACCodeException If <code>validate</code> is <code>true</code>
     *             and the new sequence contains any illegal IUPAC codes.
     */
    @Override
    public void setSequence(byte[] bases, int start, int length, boolean validate) {
        super.setSequence(bases, start, length, validate);

        referencePosition.setLength(length);
    }

    /*
     * @see
     * edu.cshl.schatz.jnomics.ob.AbstractNucleotideSequence#subSequence(int,
     * int)
     */
    @Override
    public NucleotideSequence subSequence(int start, int end) {
        return subSequence(new SequenceSegment(this), start, end);
    }

    /**
     * Returns a tab-delimited 6- or 7-field description of this segment. The
     * contents of the fields are described below.
     * 
     * <pre>
     * Col Type Regexp   Brief description
     *  1  INT  [0-9]+   Position of the 5'-most base in the current sequence 
     *                   (first base in sequence = index 0)
     *  2  INT  [0-9]+   Number of bases contained in the segment.
     *  3  STR  [\d\s]+  The name of the segment's sequence of origin. For de
     *                   novo or other sequences of unknown origin this field
     *                   will be empty.
     *  4  INT  [0-9]+   Start position in the sequence of origin 
     *  5       [\+\-]   Orientation in relative to original sequence 
     *  6  STR  [-ACGTURYKMSWBDHVNX]   The segments nucleotide sequence; only
     *                   used when origin is unknown (field 3 is empty).
     * </pre>
     */
    @Override
    public String toString() {
        return toString(
            getEndpoints(), getEndpoints().length(), referenceName, referencePosition,
            getOrientation(), referenceName.equals("") ? getSequenceString() : "");
    }
}
