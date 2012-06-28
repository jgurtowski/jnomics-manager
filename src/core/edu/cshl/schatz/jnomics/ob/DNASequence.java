/*
 * Copyright (C) 2011 Matthew A. Titmus
 */

package edu.cshl.schatz.jnomics.ob;

import java.io.PrintStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.cshl.schatz.jnomics.io.DNASequenceReader;

/**
 * <p>
 * This class represents a DNA sequence, and provides various methods that allow
 * its sequence to be altered in manners analogous to deletion, insertion, and
 * inversion structural variations. It is designed to remember the original
 * positions of each base in its original sequence (as well as those of any
 * inserted sequences, if they are stated), even after numerous modifications.
 * </p>
 * 
 * @author Matthew Titmus
 */
public class DNASequence extends AbstractNucleotideSequence implements NucleotideSequence {
    /**
     * The default position value assigned to the first bases in a
     * {@link DNASequence}s. Typically 1 or 0, unless this represents a
     * sub-sequence of a larger entity.
     */
    public static final int DEFAULT_FIRST_BASE_POSITION = 1;

    /**
     * The maximum length {@link SequenceSegment}s created by the constructors.
     * Sequences longer than this are divided among into
     * <code>(length % MAX_SEGMENT_LENGTH) + 1</code> segment objects.
     */
    static final int MAX_SEGMENT_LENGTH = Integer.MAX_VALUE;
    /**
     * The value of log(2). Since (strangely) <code>java.lang.Math</code> 
     * doesn't provide a method for performing a logarithm with an arbitrary
     * base, we use this to perform a change-of-base from log(x) to log2(x).
     */
    private static final double LOG_2 = StrictMath.log(2);

    /**
     * <p>
     * Parts of the sequence represented by this class are represented by a
     * series of {@link SequenceSegment} objects, each of which knows its
     * nucleotide sequence, it current position in the sequence, the identity of
     * its sequence of origin and original position in that sequence, and its
     * current orientation relative to its position in its sequence of origin.
     * <p>
     * These are generally not visible outside of this instance (with the
     * exception of the <code>outputSummary()</code> method), and the underlying
     * sequence is presented as an uninterrupted sequence of bases the
     */
    List<SequenceSegment> segments = new ArrayList<SequenceSegment>();

    private String referenceName = "";

    /**
     */
    public DNASequence(NucleotideSequence toCopy) {
        set(toCopy);
    }

    /**
     */
    public DNASequence(NucleotideSequence toCopy, int firstBasePosition, int length) {
        set(toCopy, firstBasePosition, length);
    }

    /**
     * @throws IUPACCodeException if an invalid nucleotide code is used (see
     *             http://www.bioinformatics.org/sms/iupac.html).
     * @param identifier
     * @param sequence
     */
    public DNASequence(String identifier, char[] sequence) {
        this(identifier, sequence, DEFAULT_FIRST_BASE_POSITION, Orientation.PLUS);
    }

    /**
     * @throws IUPACCodeException if an invalid nucleotide code is used (see
     *             http://www.bioinformatics.org/sms/iupac.html).
     * @param identifier
     * @param sequence
     * @param firstBasePosition
     * @param orient
     */
    public DNASequence(String identifier, char[] sequence, int firstBasePosition, Orientation orient) {
        final int LENGTH = sequence.length;

        SequenceSegment segment;
        char[] bases = new char[LENGTH];

        setReferenceName(identifier == null ? "" : identifier);
        getEndpoints().setFirst(firstBasePosition);

        // If the sequence.length is greater than the MAX_SEGMENT_LENGTH, we
        // divide the bases among several SequenceSegment instances.
        int i = 0;
        for (; i < (sequence.length - MAX_SEGMENT_LENGTH); i += MAX_SEGMENT_LENGTH) {
            System.arraycopy(sequence, i, bases, 0, MAX_SEGMENT_LENGTH);

            segment = new SequenceSegment();
            segment.setSequence(bases, false);
            segment.getEndpoints().setFirst(firstBasePosition + i);
            segment.getReferencePosition().setFirst(firstBasePosition + i);
            segment.setReferenceName(identifier);

            segments.add(segment);
        }

        System.arraycopy(sequence, i, bases, 0, sequence.length - i);

        segment = new SequenceSegment();
        segment.setSequence(bases, false);
        segment.getEndpoints().setFirst(firstBasePosition + i);
        segment.getReferencePosition().setFirst(firstBasePosition + i);
        segment.setReferenceName(identifier);

        segments.add(segment);
    }

    /**
     * @throws IUPACCodeException if an invalid nucleotide code is used (see
     *             http://www.bioinformatics.org/sms/iupac.html).
     * @param identifier
     * @param sequence
     */
    public DNASequence(String identifier, CharSequence sequence) {
        this(
            identifier, sequence.toString().toCharArray(), DEFAULT_FIRST_BASE_POSITION,
            Orientation.PLUS);
    }

    /**
     * @throws IUPACCodeException if an invalid nucleotide code is used (see
     *             http://www.bioinformatics.org/sms/iupac.html).
     * @param identifier
     * @param sequence
     * @param firstBasePosition
     * @param orient
     */
    public DNASequence(String identifier, CharSequence sequence, int firstBasePosition,
        Orientation orient) {

        this(identifier, sequence.toString().toCharArray(), firstBasePosition, orient);
    }

    /**
     * A simple method that returns base 2 log of <code>a</code>, since
     * (strangely) <code>java.lang.Math</code> doesn't provide a method for
     * performing a logarithm with an arbitrary base.
     * <ul>
     * <li>If the argument is NaN or less than zero, then the result is NaN.
     * <li>If the argument is positive infinity, then the result is positive
     * infinity.
     * <li>If the argument is positive zero or negative zero, then the result is
     * negative infinity.
     * </ul>
     * 
     * @param a A value.
     * @return The base 2 logarithm of a double value.
     */
    private static final double log2(double a) {
        return StrictMath.log(a) / LOG_2;
    }

    /*
     * @see java.util.List#add(int, java.lang.Object)
     */
    public void add(DNASequence sequence) {
        insert(length(), sequence.segments);
    }

    /*
     * @see java.lang.CharSequence#charAt(int)
     */
    @Override
    public char charAt(int index) {
        return 0;
    }

    /*
     * @see edu.cshl.schatz.jnomics.io.NucleotideSequence#clear()
     */
    @Override
    public void clear() {
        segments.clear();
    }

    /**
     * Generates and returns an effective deep clone that doesn't share
     * underlying data structures with its parent instance.
     * 
     * @see java.lang.Object#clone()
     */
    @Override
    public DNASequence clone() {
        DNASequence clone = null;

        try {
            clone = (DNASequence) super.clone();

            clone.segments = new ArrayList<SequenceSegment>(segments.size());

            for (SequenceSegment s : segments) {
                clone.segments.add(new SequenceSegment(s));
            }
        } catch (CloneNotSupportedException e) {
            // But it IS supported. If we're here, then a Bad Thing happened.

            throw new Error(e);
        }

        return clone;
    }

    /**
     * Deletes <code>length</code> bases from this sequence, beginning with the
     * base at position <code>firstIndex</code>.
     * 
     * @param firstIndex
     * @param length
     */
    public void delete(int firstIndex, int length) {
        int[] rangeIndexBounds = new int[2];
        ArrayList<SequenceSegment> tempRanges;

        checkBounds(this, firstIndex, length);

        rangeIndexBounds[0] = createBreakpoint(firstIndex)[1];
        rangeIndexBounds[1] = createBreakpoint(firstIndex + length)[1];

        tempRanges = new ArrayList<SequenceSegment>(segments.size());
        tempRanges.addAll(segments.subList(0, rangeIndexBounds[0]));
        tempRanges.addAll(segments.subList(rangeIndexBounds[1], segments.size()));

        segments = tempRanges;

        rebuildPositionData();
    }

    /**
     * Generates a tandem duplication. Mechanically, the bases from
     * <code>firstIndex</code> to <code>firstIndex + length - 1</code> are
     * copied and inserted at <code>firstIndex</code>.
     * 
     * @param firstIndex
     * @param length
     * @param invert
     */
    public void duplicate(int firstIndex, int length, boolean invert) {
        duplicate(firstIndex, length, firstIndex + length, invert);
    }

    /**
     * Generates a tandem duplication. Mechanically, the bases from
     * <code>firstIndex</code> to <code>firstIndex + length - 1</code> are
     * copied and inserted at <code>firstIndex</code>.
     * 
     * @param firstIndex
     * @param length
     * @param invert
     */
    public void duplicate(int firstIndex, int length, int insertBeforeIndex, boolean invert) {
        checkBounds(this, firstIndex, length);

        DNASequence duplicated = new DNASequence(this);

        duplicated.subSequence(firstIndex, length);

        if (invert) {
            duplicated.reverseComplement();
        }

        insert(insertBeforeIndex, duplicated);
    }

    /*
     * @see edu.cshl.schatz.jnomics.io.NucleotideSequence#getBases()
     */
    @Override
    public byte[] getBases() {
        byte[] bytes = new byte[length()];

        System.arraycopy(getRawBytes(), 0, bytes, 0, bytes.length);

        return bytes;
    }

    /*
     * @see edu.cshl.schatz.jnomics.AbstractNucleotideSequence#getEndpoints()
     */
    @Override
    public PositionRange getEndpoints() {
        InnerPositionRange range;

        if (!(super.getEndpoints() instanceof InnerPositionRange)) {
            range = new InnerPositionRange(
                super.getEndpoints().first(), super.getEndpoints().length());
            range.changedFlag = true;
        } else {
            range = (InnerPositionRange) super.getEndpoints();
        }

        if (range.changedFlag) {
            rebuildPositionData();
        }

        return super.getEndpoints();
    }

    /**
     * Returns the 0th-order entropy of the sequence.
     * <p>
     * TODO This method is untested; build some tests and use them!
     * 
     * @return An entropy value in the range of 0 to 2, inclusive.
     */
    public double getEntropy() {
        double entropy = 0.0;

        synchronized (this) {
            byte[] bytes = getRawBytes();
            int length = length();

            double countsA = 0.0;
            double countsC = 0.0;
            double countsT = 0.0;
            double countsG = 0.0;

            for (int i = 0; i < length; i++) {
                Base base = Base.characterToBase(bytes[i]);
                countsA += base.pA;
                countsC += base.pC;
                countsT += base.pT;
                countsG += base.pG;
            }

            double seqlen = length, freq;
            if (0 < countsA) {
                freq = countsA / seqlen;
                entropy -= freq * log2(Math.log(freq));
            }

            if (0 < countsC) {
                freq = countsC / seqlen;
                entropy -= freq * log2(Math.log(freq));
            }

            if (0 < countsT) {
                freq = countsT / seqlen;
                entropy -= freq * log2(Math.log(freq));
            }

            if (0 < countsG) {
                freq = countsG / seqlen;
                entropy -= freq * log2(Math.log(freq));
            }
        }

        return entropy;
    }

    /**
     * The maximum entropy (<i>H<sub>max</sub></i>) of a sequence of length
     * <i>length()</i> (|S|); the entropy if the constituent nucleotides were
     * uniformly distributed. It is calculated as log<sub>2</sub>|S|.
     */
    public double getEntropyMax() {
        return log2(length());
    }

    /**
     * The <i>evenness</i> of a sequence is defined as <i>H /
     * H<sub>max</sub></i> (the ratio of its entropy divided by the maximum
     * entropy value for a sequence of size <i>length()</i>).
     */
    public double getEvenness() {
        return getEntropy() / getEntropyMax();
    }

    /**
     * @deprecated Use {@link #getReferenceName()} instead.
     * @return The identifier
     */
    @Deprecated
    public String getIdentifier() {
        return getReferenceName();
    }

    /**
     * Unsupported (always returns <code>Orientation.UNSPECIFIED</code>).
     * 
     * @see edu.cshl.schatz.jnomics.ob.NucleotideSequence#getOrientation()
     */
    @Override
    public Orientation getOrientation() {
        return Orientation.UNSPECIFIED;
    }

    /**
     * Unsupported.
     * 
     * @throws UnsupportedOperationException
     * @see edu.cshl.schatz.jnomics.ob.NucleotideSequence#getRawBytes()
     */
    @Override
    public byte[] getRawBytes() {
        return null;
    }

    /*
     * @see
     * edu.cshl.schatz.jnomics.AbstractNucleotideSequence#getReferenceName()
     */
    @Override
    public String getReferenceName() {
        return null;
    }

    /**
     * Returns an instance of {@link DNASequenceReader}, an implementation of
     * {@link Reader} that allows easy buffered access to the DNA sequence this
     * object wraps. Conveniently, it also efficiently converts the internal
     * <code>byte[]</code> array to a <code>char[]</code> array without using
     * the C
     * 
     * @return DNASequenceReader
     */
    public DNASequenceReader getSequenceReader() {
        return new DNASequenceReader(this);
    }

    /*
     * @see
     * edu.cshl.schatz.jnomics.AbstractNucleotideSequence#getSequenceString()
     */
    @Override
    public String getSequenceString() {
        return getSequenceString(Integer.MAX_VALUE);
    }

    /**
     * Returns a String representation of this sequence ("ACTGACTG....").
     */
    public String getSequenceString(int maxLength) {
        int basesToGet = Math.min(length(), maxLength);
        char[] chars = new char[basesToGet];

        getSequenceReader().read(chars, 0, basesToGet);

        return new String(chars);
    }

    /**
     * Inserts the specified {@link DNASequence} into this instance, adjusting
     * all "current position" values appropriately and conserving orientations
     * and "original positions". The inserted sequence object is not modified in
     * any way, so it is possible to perform tandem insertions by calling this
     * method multiple times using the same insert sequence.
     */
    public void insert(int insertBefore, DNASequence sequence) {
        insert(insertBefore, sequence.segments);
    }

    /**
     * Inverts the specified bases within this sequence. This occurs in place,
     * directly modifying this instance and adjusting all position and
     * orientation attributes appropriately.
     */
    public void invert(int firstIndex, int length) {
        checkBounds(this, firstIndex, length);

        // Convert the DNA sequence ranges into indices within the SequenceOpts
        // range array. If the requested position falls in the center of a
        // range, this will also create break points at those positions.

        int rangeStart = createBreakpoint(firstIndex)[1];
        int rangeEnd = createBreakpoint(firstIndex + length)[1] - 1;

        SequenceSegment a, b;
        for (; rangeStart < rangeEnd; rangeStart++, rangeEnd--) {
            a = segments.get(rangeStart);
            a.reverseComplement();

            b = segments.get(rangeEnd);
            b.reverseComplement();

            segments.set(rangeStart, b);
            segments.set(rangeEnd, a);
        }

        if (rangeStart == rangeEnd) {
            SequenceSegment mid = segments.get(rangeStart);
            mid.reverseComplement();
        }

        rebuildPositionData();
    }

    /*
     * @see edu.cshl.schatz.jnomics.AbstractNucleotideSequence#length()
     */
    @Override
    public int length() {
        return getEndpoints().length();
    }

    /**
     * Generates a summary of all "segments" that compose this sequence.
     * 
     * @param out The stream to output the summary data to.
     */
    public void outputSummary(PrintStream out) {
        if (segments.size() > 0) {
            SequenceSegment prev = new SequenceSegment(segments.get(0));

            for (int i = 1; i < segments.size(); i++) {
                SequenceSegment ss = new SequenceSegment(segments.get(i));

                if (ss.follows(prev)
                        && ((prev.getReferencePosition().last() + 1) == ss.getReferencePosition().first())) {

                    prev.getReferencePosition().setEndpoints(
                        prev.getReferencePosition().first(), ss.getReferencePosition().last());

                    prev.getEndpoints().setEndpoints(
                        prev.getEndpoints().first(), ss.getEndpoints().last());
                } else {
                    out.println(prev.toString());
                    prev = ss;
                }
            }

            out.println(prev.toString());
        }
    }

    /**
     * Reverse complements this entire {@link DNASequence} in place, directly
     * modifying this instance and adjusting all position and orientation
     * attributes appropriately. Behind the scenes, this simply calls
     * <code>invert(0, length())</code>.
     */
    @Override
    public void reverseComplement() {
        invert(getEndpoints().first(), length());
    }

    /*
     * @see
     * edu.cshl.schatz.jnomics.io.NucleotideSequence#set(edu.cshl.schatz.jnomics
     * .io.NucleotideSequence)
     */
    @Override
    public void set(NucleotideSequence sequence) {
        clear();

        setReferenceName(sequence.getReferenceName());
        getEndpoints().set(sequence.getEndpoints());

        if (sequence instanceof DNASequence) {
            DNASequence dnaSequence = (DNASequence) sequence;

            for (SequenceSegment ss : dnaSequence.segments) {
                segments.add(new SequenceSegment(ss));
            }
        } else {
            segments.add(new SequenceSegment(sequence));
        }
    }

    /*
     * @see
     * edu.cshl.schatz.jnomics.io.NucleotideSequence#set(edu.cshl.schatz.jnomics
     * .io.NucleotideSequence)
     */
    public void set(NucleotideSequence sequence, int start, int length) {
        checkBounds(sequence, start, length);

        if (sequence instanceof DNASequence) {
            DNASequence dnaSequence = (DNASequence) sequence;
            int boundLeft = dnaSequence.createBreakpoint(start)[1];
            int boundRight = dnaSequence.createBreakpoint(start + length)[1];

            segments = dnaSequence.segments.subList(boundLeft, boundRight);
        } else if (sequence instanceof SequenceSegment) {
            segments.clear();
            segments.add((SequenceSegment) sequence);
        } else {
            segments.clear();
            segments.add(new SequenceSegment(sequence));
        }

        setReferenceName(sequence.getReferenceName());
        getEndpoints().setFirst(start);
    }

    /**
     * Unsupported.
     * 
     * @throws UnsupportedOperationException
     * @see edu.cshl.schatz.jnomics.ob.NucleotideSequence#setOrientation(edu.cshl.schatz.jnomics.ob.Orientation)
     */
    @Override
    public void setOrientation(Orientation orientation) {
        throw new UnsupportedOperationException();
    }

    /**
     * Unsupported.
     * 
     * @throws UnsupportedOperationException
     * @see edu.cshl.schatz.jnomics.ob.NucleotideSequence#setRawBytes(byte[],
     *      int, int)
     */
    @Override
    public void setRawBytes(byte[] bases, int start, int length) {
        throw new UnsupportedOperationException();
    }

    /*
     * @see
     * edu.cshl.schatz.jnomics.io.NucleotideSequence#setReferenceName(java.lang
     * .String)
     */
    @Override
    public void setReferenceName(String referenceName) {
        this.referenceName = referenceName;
    }

    /*
     * @see
     * edu.cshl.schatz.jnomics.ob.AbstractNucleotideSequence#subSequence(edu
     * .cshl.schatz.jnomics.ob.AbstractNucleotideSequence, int, int)
     */
    @Override
    public DNASequence subSequence(AbstractNucleotideSequence writeTo, int start, int end) {
        int length = (end - start) + 1;

        checkBounds(this, start, length);

        if (!(writeTo instanceof DNASequence)) {
            throw new UnsupportedOperationException(
                "Currently can only write SequenceOpts subsequence into type SequenceOpts.");
        }

        DNASequence sequence = (DNASequence) writeTo;

        int boundA = createBreakpoint(start)[1];
        int boundB = createBreakpoint(start + length)[1];

        sequence.segments = segments.subList(boundA, boundB);
        sequence.getEndpoints().setFirst(start);

        return sequence;
    }

    /*
     * @see
     * edu.cshl.schatz.jnomics.ob.AbstractNucleotideSequence#subSequence(int,
     * int)
     */
    @Override
    public NucleotideSequence subSequence(int start, int end) {
        return subSequence(new DNASequence(this), start, end);
    }

    /**
     * Returns a short descriptive {@link String}. To get a String
     * representation of the sequence ("ACTG...") use
     * {@link DNASequence#getSequenceString()}
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        String s = null;

        s = getSequenceReader().readLine();

        if ((s == null) || (s.length() < 1)) {
            s = referenceName + " [" + length() + "bp]";
        } else if (s.length() > 40) {
            s = referenceName + " [" + length() + "bp: "
                    + s.substring(getEndpoints().first(), getEndpoints().first() + 40) + "..."
                    + "]";
        } else {
            s = referenceName + " [" + length() + "bp: " + s + "]";
        }

        return s;
    }

    /**
     * Breaks the range object at the specified position P by replacing the
     * SequenceRange [a..P..z] with two SequenceRanges [a..P-1], [P..z]. Returns
     * a two-element list with the indices of the resulting createBreakpoint
     * elements. If the requested createBreakpoint position already exists at a
     * break point then no change will be made, and the returned list will
     * contain two identical values equal to the position of the existing
     * element.
     */
    int[] createBreakpoint(int insertBefore) {
        int indices[] = null;

        // If the position is the one following the last base...
        if ((insertBefore - getEndpoints().first()) == length()) {
            indices = new int[] { segments.size(), segments.size() };
        } else {
            int rangeIndex;
            SequenceSegment range;
            PositionRange position;

            rangeIndex = sequenceIndexToRangeIndex(insertBefore);
            range = segments.get(rangeIndex);
            position = range.getEndpoints();

            if (insertBefore == position.first()) {
                // If there is already a break at this position, then we
                // don't need to create any new breaks.

                indices = new int[] { rangeIndex, rangeIndex };
            } else {
                // If the requested break position falls inside one of the
                // ranges, we replace that range with the new "broken"
                // parts.

                SequenceSegment[] newParts = range.breakAt(insertBefore);

                assert newParts.length == 2 : "range.break gave an "
                        + "unexpected number of parts.";

                indices = new int[] { rangeIndex, rangeIndex + 1 };

                segments.set(rangeIndex, newParts[1]);
                segments.add(rangeIndex, newParts[0]);
            }
        }

        return indices;
    }

    /*
     * @see java.util.List#add(int, java.lang.Object)
     */
    private void insert(int insertBeforeIndex, List<? extends SequenceSegment> c) {
        try {
            checkBounds(this, insertBeforeIndex, 1);

            // If necessary, create a new breakpoint at the insertion point.
            // The insertion point in the ranges list is returned by this
            // method.

            segments.addAll(createBreakpoint(insertBeforeIndex)[1], c);
        } catch (SequenceIndexOutOfBoundsException e) {
            try {
                checkBounds(this, insertBeforeIndex - 1, 1);

                // If we got here, then the sequence is simply to be
                // concatenated. Add the new sequence to the end of the list.

                segments.addAll(c);
            } catch (SequenceIndexOutOfBoundsException e2) {
                throw e;
            }
        }

        rebuildPositionData();
    }

    /**
     * Recalculates the "current positions" of each of the
     * {@link SequenceSegment} elements in the segments list, as well as the
     * total sequence length and GC ratio. It should be called by any method
     * that induces a variation of any kind.
     */
    private void rebuildPositionData() {
        int start = super.getEndpoints().first(), len = 0;
        Iterator<SequenceSegment> iter = segments.iterator();

        while (iter.hasNext()) {
            SequenceSegment seg = iter.next();

            if (seg.length() > 0) {
                seg.getEndpoints().setFirst(start);

                len += seg.length();
                start = seg.getEndpoints().last() + 1;
            } else {
                iter.remove();
            }
        }

        super.getEndpoints().setLength(len);
    }

    /**
     * This method searches for the {@link SequenceSegment} object in the ranges
     * <code>ranges</code> list that contains the base at the specified index
     * and returns the relevant <code>ranges</code> index.
     */
    private int sequenceIndexToRangeIndex(int sequenceIndex) {
        int first = getEndpoints().first();
        int last = getEndpoints().last() - 1;
        int rangeIndex = -1;

        if ((sequenceIndex < first) || (sequenceIndex > last)) {
            throw new OutOfRangeException("Sequence index " + sequenceIndex + " out of bounds ["
                    + first + ".." + last + "]");
        } else {
            for (int i = 0; (rangeIndex < 0) && (i < segments.size()); i++) {
                if (segments.get(i).getEndpoints().contains(sequenceIndex)) {
                    rangeIndex = i;
                }
            }
        }

        return rangeIndex;
    }

    private static class InnerPositionRange extends PositionRange {
        boolean changedFlag = false;

        /**
         * @param firstIndex
         * @param length
         */
        protected InnerPositionRange(int firstIndex, int length) {
            super(firstIndex, length);
        }

        /*
         * @see
         * edu.cshl.schatz.jnomics.util.PositionRange#set(edu.cshl.schatz.jnomics
         * .util.PositionRange)
         */
        @Override
        public void set(PositionRange toCopy) {
            super.set(toCopy);

            changedFlag = true;
        }

        /*
         * @see edu.cshl.schatz.jnomics.util.PositionRange#setEndpoints(int,
         * int)
         */
        @Override
        public void setEndpoints(int firstIndex, int lastIndex) {
            super.setEndpoints(firstIndex, lastIndex);

            changedFlag = true;
        }

        /*
         * @see edu.cshl.schatz.jnomics.util.PositionRange#setFirst(int)
         */
        @Override
        public void setFirst(int firstIndex) {
            super.setFirst(firstIndex);

            changedFlag = true;
        }
    }
}
