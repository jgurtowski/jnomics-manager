/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.ob;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

/**
 * <p>
 * This class represents a sequence of nucleotides, and provides methods for
 * their manipulation and analysis.
 * </p>
 * 
 * @author Matthew A. Titmus
 */
public abstract class AbstractNucleotideSequence implements NucleotideSequence {
    /**
     * If this sequence maps to a reference sequence (hg19, for example), this
     * points to the first range it covers. If it doesn't, then this simply gets
     * set to index 1.
     */
    private PositionRange endpoints = new FixedLengthPositionRange();

    /**
     * The object's hashCode. A value of Integer.MAX_VALUE indicates an unset
     * value.
     */
    private int hashCode = Integer.MAX_VALUE;

    /**
     * The orientation of this sequence relative to the reference. Default is
     * <code>Orientation.PLUS</code>.
     */
    private Orientation orientation = Orientation.PLUS;

    /**
     * Temporary values used for GC ratio calculations.
     */
    private transient double sumAT = Double.NEGATIVE_INFINITY, sumGC = Double.NEGATIVE_INFINITY;

    /**
     * Creates a new instance with no sequence information.
     */
    public AbstractNucleotideSequence() {}

    /*
     * @see java.lang.CharSequence#charAt(int)
     */
    public char charAt(int index) {
        checkBounds(this, index, 1);

        return (char) getRawBytes()[index - getEndpoints().first()];
    }

    /*
     * @see edu.cshl.schatz.jnomics.util.NucleotideSequence#clear()
     */
    public void clear() {
        endpoints.setFirst(0);
        endpoints.setLength(0);
        orientation = Orientation.PLUS;
        setReferenceName("");

        hashCode = Integer.MAX_VALUE;
        sumGC = sumAT = Double.NEGATIVE_INFINITY;
    }

    /**
     * Returns <code>true</code> if <code>obj</code> is an instance of
     * {@link AbstractNucleotideSequence}, and is identical to this instance for
     * sequence, position, and orientation.
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof NucleotideSequence) {
            NucleotideSequence seq = (NucleotideSequence) obj;

            return endpoints.equals(seq.getEndpoints()) && (orientation == seq.getOrientation())
                    && Arrays.equals(getBases(), seq.getBases());
        } else {
            return super.equals(obj);
        }
    }

    /*
     * @see edu.cshl.schatz.jnomics.io.NucleotideSequence#getATSum()
     */
    public double getATSum() {
        if (sumAT == Double.NEGATIVE_INFINITY) {
            buildGCSums();
        }

        return sumAT;
    }

    /*
     * @see edu.cshl.schatz.jnomics.util.NucleotideSequence#getBases()
     */
    public byte[] getBases() {
        byte[] bytes = new byte[length()];

        System.arraycopy(getRawBytes(), 0, bytes, 0, bytes.length);

        return bytes;
    }

    /*
     * @see edu.cshl.schatz.jnomics.util.NucleotideSequence#getEndpoints()
     */
    public PositionRange getEndpoints() {
        return endpoints;
    }

    /*
     * @see edu.cshl.schatz.jnomics.util.NucleotideSequence#getGCContent()
     */
    public double getGCContent() {
        double gc = getGCSum();
        double at = getATSum();

        return gc / (gc + at);
    }

    /*
     * @see edu.cshl.schatz.jnomics.io.NucleotideSequence#getGCSum()
     */
    public double getGCSum() {
        if (sumAT == Double.NEGATIVE_INFINITY) {
            buildGCSums();
        }

        return sumGC;
    }

    /*
     * @see edu.cshl.schatz.jnomics.util.NucleotideSequence#getOrientation()
     */
    public Orientation getOrientation() {
        return orientation;
    }

    /*
     * @see edu.cshl.schatz.jnomics.util.NucleotideSequence#getRawBytes()
     */
    public abstract byte[] getRawBytes();

    /*
     * @see edu.cshl.schatz.jnomics.util.NucleotideSequence#getReferenceName()
     */
    public abstract String getReferenceName();

    /*
     * @see edu.cshl.schatz.jnomics.util.NucleotideSequence#getSequenceString()
     */
    public abstract String getSequenceString();

    /*
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        if (hashCode == Integer.MAX_VALUE) {
            hashCode = getSequenceString().hashCode();
        }

        return hashCode;
    }

    /**
     * The number of sequence positions occupied by this sequence (the number of
     * bases plus the number of masked bases and gaps).
     * <p>
     * The implementation of this method {@link AbstractNucleotideSequence}
     * returns the value of <code>{@link #getBases()}.length</code>, which is
     * suboptimal because it first requires that a new byte array be allocated
     * and filled. Subclasses are strongly encouraged to override this method
     * with a more efficient approach.
     * 
     * @see NucleotideSequence#length()
     */
    public int length() {
        return getBases().length;
    }

    /**
     * TODO Move the read methods into a "NucleotideSequenceInputStream"? See
     * the deprecated <code>DNASequenceInputStream</code> class.
     * <p>
     * implementation? A byte-array read method intended to be identical in all
     * respects to the {@link InputStream#read(byte[]) method of the same name}
     * in the standard library {@link InputStream} class.
     * <p>
     * In summary: reads some number of nucleotide-representing bytes from the
     * underlying sequence representation and stores them into the buffer array
     * <code>b</code>. The number of bytes actually read is returned as an
     * integer. If applicable, implementations of this method should block until
     * input data is available, the end of the sequence is reached, or an
     * exception is thrown.
     * <p>
     * The <code>read(b)</code> method has the same effect as:
     * 
     * <pre>
     * 
     * <code> {@link #read(byte[], int, int) read(b, 0, b.length)}</code>
     * </pre>
     * 
     * @param b the buffer into which the data is read.
     * @return the total number of bytes read into the buffer, or
     *         <code>-1</code> is there is no more data because the end of the
     *         stream has been reached.
     * @exception IOException If the first byte cannot be read for any reason
     *                other than the end of the sequence, if the input stream
     *                has been closed, or if some other I/O error occurs.
     * @exception NullPointerException if <code>b</code> is <code>null</code>.
     * @see java.io.InputStream#read(byte[], int, int)
     */
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    /**
     * TODO Move the read methods into a "NucleotideSequenceInputStream"? See
     * the deprecated <code>DNASequenceInputStream</code> class.
     * <p>
     * A byte-array read method intended to be identical in all respects to the
     * {@link InputStream#read(byte[]) method of the same name} in the standard
     * library {@link InputStream} class.
     * <p>
     * In summary: reads some number of nucleotide-representing bytes from the
     * underlying sequence representation and stores them into the buffer array
     * <code>b</code>. The number of bytes actually read is returned as an
     * integer. If applicable, implementations of this method should block until
     * input data is available, the end of the sequence is reached, or an
     * exception is thrown.
     * <p>
     * The first byte read is stored into element <code>b[off]</code>, the next
     * one into <code>b[off+1]</code>, and so on. The number of bytes read is,
     * at most, equal to <code>len</code>. Let <i>k</i> be the number of bytes
     * actually read; these bytes will be stored in elements <code>b[off]</code>
     * through <code>b[off+</code><i>k</i><code>-1]</code>, leaving elements
     * <code>b[off+</code><i>k</i><code>]</code> through
     * <code>b[off+len-1]</code> unaffected.
     * <p>
     * In every case, elements <code>b[0]</code> through <code>b[off]</code> and
     * elements <code>b[off+len]</code> through <code>b[b.length-1]</code> are
     * unaffected.
     * 
     * @param b the buffer into which the data is read.
     * @return the total number of bytes read into the buffer, or
     *         <code>-1</code> is there is no more data because the end of the
     *         stream has been reached.
     * @exception IOException If the first byte cannot be read for any reason
     *                other than the end of the sequence, if the input stream
     *                has been closed, or if some other I/O error occurs.
     * @exception NullPointerException if <code>b</code> is <code>null</code>.
     * @see java.io.InputStream#read(byte[], int, int)
     */
    public int read(byte[] b, int off, int len) throws IOException {
        throw new UnsupportedOperationException(
            "Create a NucleotideSequenceInputStream, put this method there, and implement it.");
    }

    /**
     * Repositions the sequence such that its start (left-most) index is placed
     * at the specified position.
     */
    public void reposition(int newFirstIndex) {
        getEndpoints().setFirst(newFirstIndex);
    }

    /*
     * @see edu.cshl.schatz.jnomics.util.NucleotideSequence#reverseComplement()
     */
    public void reverseComplement() {
        // Perform an in-place reversal of the array contents, reverse
        // complementing in the process.

        Base base5, base3;
        int pos5 = 0, pos3 = length() - 1;
        byte[] bytes = getRawBytes();

        for (; pos5 < pos3; pos5++, pos3--) {
            base5 = Base.characterToBase(bytes[pos5]);
            base3 = Base.characterToBase(bytes[pos3]);

            try {
                bytes[pos3] = base5.reverseComplementCode;
            } catch (NullPointerException e) {
                throw new RuntimeException("Illegal nucleotide code: " + (char) bytes[pos5]);
            }

            try {
                bytes[pos5] = base3.reverseComplementCode;
            } catch (NullPointerException e) {
                throw new RuntimeException("Illegal nucleotide code: " + (char) bytes[pos3]);
            }
        }

        // Don't forget the one in the center!

        if (pos3 == pos5) {
            try {
                bytes[pos5] = Base.characterToBase(bytes[pos5]).reverseComplementCode;
            } catch (NullPointerException e) {
                throw new RuntimeException("Illegal nucleotide code: " + (char) bytes[pos5]);
            }
        }

        orientation = orientation.invert();
    }

    /*
     * @see
     * edu.cshl.schatz.jnomics.util.NucleotideSequence#set(edu.cshl.schatz.jnomics
     * .util.NucleotideSequence)
     */
    public void set(NucleotideSequence sequence) {
        setSequence(sequence.getRawBytes(), false);
        endpoints.set(sequence.getEndpoints());
        setOrientation(sequence.getOrientation());
        setReferenceName(sequence.getReferenceName());
    }

    /*
     * @see
     * edu.cshl.schatz.jnomics.util.NucleotideSequence#setOrientation(edu.cshl
     * .schatz.jnomics.util.Orientation)
     */
    public void setOrientation(Orientation orientation) {
        this.orientation = orientation;
        hashCode = Integer.MAX_VALUE;
    }

    /**
     * Directly sets the underlying sequence bytes to the contents of
     * <code>bases</code>. It is equivalent to
     * {@link #setRawBytes(byte[], int, int) setRawBytes(bases, 0,
     * bases.length)}
     * <p>
     * This is designed to be a speedy, system-level operation and provides no
     * validation or safety checks, and should be used with caution. Whether the
     * array is used directly or copied into an internal buffer is not
     * specified.
     * 
     * @param bases The nucleotides sequence as an array of 8-bit IUPAC
     *            character codes (case-insensitive).
     * @throws ArrayStoreException if an element in the <code>src</code> array
     *             could not be stored into the <code>dest</code> array because
     *             of a type mismatch.
     * @throws IndexOutOfBoundsException if copying would cause access of data
     *             outside array bounds.
     * @throws NullPointerException if either <code>bases</code> or the internal
     *             buffer is <code>null</code>.
     */
    public void setRawBytes(byte[] bases) {
        setRawBytes(bases, 0, bases.length);
    }

    /*
     * @see edu.cshl.schatz.jnomics.ob.NucleotideSequence#setRawBytes(byte[],
     * int, int)
     */
    public abstract void setRawBytes(byte[] bases, int start, int length);

    /*
     * @see
     * edu.cshl.schatz.jnomics.util.NucleotideSequence#setReferenceName(java
     * .lang.String)
     */
    public abstract void setReferenceName(String referenceName);

    /**
     * Resets the nucleotide sequence to the values contained in the specified
     * <code>byte</code> array.
     * 
     * @param bases The nucleotides sequence as an array of 8-bit IUPAC
     *            character codes (case-insensitive).
     * @param validate If <code>true</code>, the contents of bases
     *            <code>bases</code> are validated (faster); otherwise the
     *            object's bases value is simply replaced.
     * @throws IUPACCodeException If <code>validate</code> is <code>true</code>
     *             and the new sequence contains any illegal IUPAC codes.
     */
    public void setSequence(byte[] bases, boolean validate) {
        setSequence(bases, 0, bases.length, validate);
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
    public void setSequence(byte[] bases, int start, int length, boolean validate) {
        setRawBytes(bases, start, length);

        if (validate) {
            for (int i = start; i < (start + length); i++) {
                if (null == Base.characterToBase(bases[i])) {
                    throw new IUPACCodeException("Illegal nucleotide code: " + (char) bases[i]);
                }
            }
        }

        sumGC = sumAT = Double.NEGATIVE_INFINITY;
        hashCode = Integer.MAX_VALUE;

        endpoints.setLength(bases.length);
    }

    /**
     * Resets the nucleotide sequence.
     * 
     * @param bases The nucleotides sequence as an array of IUPAC character
     *            codes (case-insensitive).
     * @throws IUPACCodeException If the new sequence contains any illegal IUPAC
     *             codes.
     */
    public void setSequence(char[] bases, boolean validate) {
        setSequence(bases, 0, bases.length, validate);
    }

    /**
     * Resets the nucleotide sequence.
     * 
     * @param bases The nucleotides sequence as an array of IUPAC character
     *            codes (case-insensitive).
     * @throws IUPACCodeException If the new sequence contains any illegal IUPAC
     *             codes.
     */
    public void setSequence(char[] bases, int start, int length, boolean validate) {
        byte[] bytes = new byte[length];

        for (int i = start; i < (start + length); i++) {
            bytes[i - start] = (byte) bases[i];
        }

        setSequence(bytes, validate);
    }

    /**
     * Resets the nucleotide sequence.
     * 
     * @param bases The nucleotides sequence as an array of 8-bit IUPAC
     *            character codes (case-insensitive).
     * @param validate If <code>true</code>, the contents of bases
     *            <code>bases</code> are validated (faster); otherwise the
     *            object's bases value is simply replaced.
     * @throws IUPACCodeException If <code>validate</code> is <code>true</code>
     *             and the new sequence contains any illegal IUPAC codes.
     */
    public void setSequence(String bases, boolean validate) {
        setSequence(bases.toCharArray(), 0, bases.length(), validate);
    }

    /**
     * Shifts the start position the specified number of places to the left (in
     * the 5' direction).
     */
    public void shiftPositionLeft(int number) {
        reposition(getEndpoints().first() - number);
    }

    /**
     * Shifts the start position the specified number of places to the right (in
     * the 3' direction).
     */
    public void shiftPositionRight(int number) {
        reposition(getEndpoints().first() + number);
    }

    /**
     * Sets the specified sequence to a subsequence of this sequence from the
     * position indicated. The subsequence starts with the value at the
     * specified index and ends with the value at index <tt>end - 1</tt>. The
     * length (in <code>char</code>s) of the returned sequence is
     * <tt>end - start</tt>, so if <tt>start == end</tt> then an empty sequence
     * is returned. </p>
     * 
     * @param start the start index, inclusive
     * @param end the end index, exclusive
     * @throws SequenceIndexOutOfBoundsException if <tt>start</tt> is greater
     *             than <tt>end</tt>, or either is less than
     *             <code>getEndpoints().start</code> or greater than or equal to
     *             getEndpoints().end</code>
     */
    public NucleotideSequence subSequence(AbstractNucleotideSequence writeTo, int start, int end) {
        checkBounds(this, start, start - end);
        writeTo.setSequence(getRawBytes(), start, start - end, false);
        writeTo.getEndpoints().setFirst(start);

        return writeTo;
    }

    /**
     * Returns a new <code>NucleotideSequence</code> that is a subsequence of
     * this sequence from the position indicated. The subsequence starts with
     * the value at the specified index and ends with the value at index
     * <tt>end - 1</tt>. The length (in <code>char</code>s) of the returned
     * sequence is <tt>end - start</tt>, so if <tt>start == end</tt> then an
     * empty sequence is returned. </p>
     * 
     * @param start the start index, inclusive
     * @param end the end index, exclusive
     * @throws SequenceIndexOutOfBoundsException if <tt>start</tt> is greater
     *             than <tt>end</tt>, or either is less than
     *             <code>getEndpoints().start</code> or greater than or equal to
     *             getEndpoints().end</code>
     */
    public abstract NucleotideSequence subSequence(int start, int end);

    /**
     * Returns a {@link String} representation the contained nucleotide
     * sequence; it is equivalent to {@link #getSequenceString()}.
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return getSequenceString();
    }

    /**
     * Check whether the a range starting at index <code>first</code> and
     * extending <code>length</code> bases exists entirely within this sequence.
     * If it does, then this method always returns <code>true</code>.
     * 
     * @throws SequenceIndexOutOfBoundsException if the range extends beyond the
     *             currently defined boundaries of this sequence.
     */
    protected boolean checkBounds(NucleotideSequence sequence, int start, int length) {
        if (!sequence.getEndpoints().contains(start, length)) {
            throw new SequenceIndexOutOfBoundsException("Range [" + start + ".." + (start + length)
                    + "] extends outside [" + sequence.getEndpoints().first() + ".."
                    + sequence.getEndpoints().last() + "]");
        }

        return true;
    }

    /**
     * Sets the endpoints. This is intended to be used by constructors; routine
     * endpoint modification should be done by manipulating the
     * {@link #getEndpoints() } object.
     */
    protected void setEndpoints(PositionRange endpoints) {
        this.endpoints = endpoints;
    }

    /**
     * Recalculates the GC and AT sums, typically as a first step towards
     * calculating GC ratio.
     */
    private void buildGCSums() {
        sumGC = sumAT = 0;

        for (int i = 0; i < endpoints.length(); i++) {
            Base base = Base.characterToBase(getRawBytes()[i]);

            sumGC += base.gcRatioWeight;
            sumAT += 1 - base.gcRatioWeight;
        }
    }

    /**
     * An implementation of {@link PositionRange} in which its first position
     * may be freely modified, but its length is drawn from the length of the
     * {@link NucleotideSequence} that owns it.
     * <p>
     * Any attempts to modify the length or last base position of this range
     * will result in an {@link UnsupportedOperationException} being thrown.
     * 
     * @author Matthew A. Titmus
     */
    public class FixedLengthPositionRange extends PositionRange {
        /**
         * Creates a new {@link PositionRange} instance with a first position
         * (the index of the left-most base in the sequence) of 1 and an
         * unmodifiable length taken from {@link NucleotideSequence#length()}.
         */
        FixedLengthPositionRange() {
            super(DEFAULT_FIRST_POSITION, AbstractNucleotideSequence.this.length());
        }

        /**
         * Creates a new {@link PositionRange} implementation with a specified
         * first position (the index of the left-most base in the sequence). The
         * length property is taken from {@link NucleotideSequence#length()}.
         * 
         * @param first The position of the left-most base in the sequence.
         */
        FixedLengthPositionRange(int first) {
            super(first, AbstractNucleotideSequence.this.length());
        }

        /**
         * Returns the total length of the outer
         * {@link AbstractNucleotideSequence} instance, equal to
         * {@link AbstractNucleotideSequence#length()} and
         * <code>{@link #last()} - {@link #first()} + 1</code>.
         * 
         * @return The total length of the outer
         *         {@link AbstractNucleotideSequence} instance.
         * @see edu.cshl.schatz.jnomics.ob.PositionRange#length()
         */
        @Override
        public int length() {
            return AbstractNucleotideSequence.this.length();
        }

        /*
         * @see
         * edu.cshl.schatz.jnomics.ob.PositionRange#set(edu.cshl.schatz.jnomics
         * .ob.PositionRange)
         */
        @Override
        protected void set(PositionRange toCopy) {
            throw new UnsupportedOperationException(
                "The length of a FixedLengthPositionRange may not be modified.");
        }

        /*
         * @see edu.cshl.schatz.jnomics.ob.PositionRange#setEndpoints(int, int)
         */
        @Override
        protected void setEndpoints(int first, int last) {
            throw new UnsupportedOperationException(
                "The length of a FixedLengthPositionRange may not be modified.");
        }

        /*
         * @see edu.cshl.schatz.jnomics.ob.PositionRange#setLength(int)
         */
        @Override
        protected void setLength(int length) {
            throw new UnsupportedOperationException(
                "The length of a FixedLengthPositionRange may not be modified.");
        }
    }
}
