/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.ob.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import edu.cshl.schatz.jnomics.ob.AbstractNucleotideSequence;
import edu.cshl.schatz.jnomics.ob.NucleotideSequence;
import edu.cshl.schatz.jnomics.ob.Orientation;
import edu.cshl.schatz.jnomics.ob.PositionRange;

/**
 * <p>
 * This class represents a sequence of nucleotides, and provides methods for
 * their manipulation and analysis.
 * </p>
 * 
 * @author Matthew Titmus
 */
public class NucleotideSequenceWritable extends AbstractNucleotideSequence
        implements NucleotideSequence, Writable, CharSequence {

    Text baseBuffer = new Text();

    /**
     * The name of the reference sequence, if any. By default, this is set to an
     * empty string.
     */
    private Text referenceName = new Text();

    public NucleotideSequenceWritable() {
        super();

        setEndpoints(new PositionRangeWritable(getEndpoints()));
    }

    /*
     * @see edu.cshl.schatz.jnomics.io.AbstractNucleotideSequence#getBytes()
     */
    @Override
    public byte[] getRawBytes() {
        return baseBuffer.getBytes();
    }

    /*
     * @see
     * edu.cshl.schatz.jnomics.io.AbstractNucleotideSequence#getReferenceName()
     */
    @Override
    public String getReferenceName() {
        return referenceName.toString();
    }

    /**
     * Gets the name of the reference sequence, such as "chr21", or "NC_12345".
     * If not set, this will contain an empty Text instance.
     */
    public Text getReferenceNameText() {
        return referenceName;
    }

    /*
     * @see
     * edu.cshl.schatz.jnomics.io.AbstractNucleotideSequence#getSequenceString()
     */
    @Override
    public String getSequenceString() {
        return baseBuffer.toString();
    }

    /*
     * @see edu.cshl.schatz.jnomics.ob.AbstractNucleotideSequence#length()
     */
    @Override
    public int length() {
        return baseBuffer != null ? baseBuffer.getLength() : 0;
    }

    /*
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    public void readFields(DataInput in) throws IOException {
        PositionRangeWritable endPoints = (PositionRangeWritable) getEndpoints();

        referenceName.readFields(in);
        endPoints.readFields(in);
        baseBuffer.readFields(in);
        setOrientation(in.readBoolean() ? Orientation.PLUS : Orientation.MINUS);

        setSequence(baseBuffer.getBytes(), false);
    }

    /*
     * @see
     * edu.cshl.schatz.jnomics.io.AbstractNucleotideSequence#setSequence(byte[],
     * int, int)
     */
    @Override
    public void setRawBytes(byte[] bases, int start, int length) {
        baseBuffer.set(bases, start, length);
    }

    /**
     * Sets the name of the reference sequence, such as "chr21", or "NC_12345".
     */
    @Override
    public void setReferenceName(String referenceName) {
        this.referenceName.set(referenceName);
    }

    /**
     * Sets the name of the reference sequence, such as "chr21", or "NC_12345".
     */
    public void setReferenceName(Text referenceName) {
        this.referenceName.set(referenceName);
    }

    /*
     * @see edu.cshl.schatz.jnomics.util.NucleotideSequence#subSequence(int,
     * int)
     */
    @Override
    public NucleotideSequence subSequence(int start, int end) {
        NucleotideSequenceWritable sequence = new NucleotideSequenceWritable();
        int length = (start - end) + 1;

        checkBounds(this, start, length);

        sequence.setSequence(getRawBytes(), start, length, false);
        sequence.reposition(start);

        return sequence;
    }

    /*
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    public void write(DataOutput out) throws IOException {
        PositionRangeWritable endPoints = (PositionRangeWritable) getEndpoints();

        referenceName.write(out);
        endPoints.write(out);
        baseBuffer.write(out);
        out.writeBoolean(getOrientation() == Orientation.PLUS);
    }

    private class PositionRangeWritable extends PositionRange implements Writable {
        PositionRangeWritable(PositionRange range) {
            super(range.first(), range.length());
        }

        /*
         * @see edu.cshl.schatz.jnomics.ob.PositionRange#length()
         */
        @Override
        public int length() {
            return baseBuffer.getLength();
        }

        /*
         * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
         */
        public void readFields(DataInput in) throws IOException {
            setFirst(in.readInt());
            setLength(in.readUnsignedShort());
        }

        /*
         * @see
         * edu.cshl.schatz.jnomics.ob.PositionRange#set(edu.cshl.schatz.jnomics
         * .ob.PositionRange)
         */
        @Override
        public void set(PositionRange toCopy) {
            setFirst(toCopy.first());
        }

        /*
         * @see edu.cshl.schatz.jnomics.ob.PositionRange#setEndpoints(int, int)
         */
        @Override
        public void setEndpoints(int firstIndex, int lastIndex) {
            setFirst(firstIndex);
        }

        /*
         * @see edu.cshl.schatz.jnomics.ob.PositionRange#setLength(int)
         */
        @Override
        public void setLength(int length) {
            /* No-op */
        }

        /*
         * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
         */
        public void write(DataOutput out) throws IOException {
            out.writeInt(first());
            out.writeShort(length());
        }
    }
}
