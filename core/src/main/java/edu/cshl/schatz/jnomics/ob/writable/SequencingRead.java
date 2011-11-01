/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.ob.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import edu.cshl.schatz.jnomics.ob.NucleotideSequence;

/**
 * <ul>
 * <li>Template: The DNA/RNA sequence partially sequenced on a sequencing
 * machine or assembled from raw sequences. In paired-end or mate-pair data, a
 * template will generally consist of two reads (one for each end of the region
 * sequenced).
 * <li>Template length: The number bases in the region between the leftmost
 * mapped base of the first read and the rightmost base of the last read
 * (inclusive), including the bases in the gap between the reads.
 * </ul>
 * 
 * @author Matthew Titmus
 */
public class SequencingRead extends NucleotideSequenceWritable
        implements WritableComparable<SequencingRead> {

    public static final int FLAG_DUPLICATE = 0x400;
    public static final int FLAG_EACH_PROPERLY_ALIGNED = 0x002;
    public static final int FLAG_FIRST_SEGMENT = 0x040;
    public static final int FLAG_LAST_SEGMENT = 0x080;
    public static final int FLAG_MULTIPLE_FRAGMENTS = 0x001;
    public static final int FLAG_NEXT_REVERSE_COMPLEMENTED = 0x020;
    public static final int FLAG_NEXT_UNMAPPED = 0x008;
    public static final int FLAG_REVERSE_COMPLEMENTED = 0x010;
    public static final int FLAG_SECONDARY_ALIGNMENT = 0x100;
    public static final int FLAG_SUBQUALITY = 0x200;
    public static final int FLAG_UNMAPPED = 0x004;

    private static final int UNDEFINED_INT = 0;

    private static final String UNDEFINED_STRING = "";

    /**
     * SAM column 6: CIGAR string
     */
    private Text cigarString = new Text(UNDEFINED_STRING);

    /**
     * SAM column 2: The SAM-style flags value.
     */
    private int flags = UNDEFINED_INT;

    /**
     * SAM column 5: Mapping quality
     */
    private int mappingQuality = UNDEFINED_INT;

    /**
     * SAM column 7: The mate/next fragment's 1-based mapping position.
     */
    private int nextPosition = UNDEFINED_INT;

    /**
     * SAM column 7: The name of mate/next fragment's reference sequence, if
     * any.
     */
    private Text nextReferenceName = new Text(UNDEFINED_STRING);

    /**
     * SAM column 11: ASCII of Phred-scaled base QUALity+33
     */
    private Text phredString = new Text(UNDEFINED_STRING);

    /**
     * Arbitrary properties. The implementation is dependent on the original
     * format. In a SAM record, for example, this would be where the optional
     * tags are placed.
     */
    private ReadProperties properties = new ReadProperties();

    /**
     * The query template that this read is a member of. This value doesn't get
     * stored during a write() operation; rather, it gets read/written only by a
     * QueryTemplate read/write.
     */
    private QueryTemplate queryTemplate = null;

    /**
     * SAM column 1: Query template NAME
     */
    private Text readName = new Text(UNDEFINED_STRING);

    public SequencingRead() {
        super();

        reposition(UNDEFINED_INT);
    }

    public SequencingRead(SequencingRead toClone) {
        this();

        set(toClone);
    }

    /**
     * Resets all fields in this instance to their default value.
     * 
     * @see edu.cshl.schatz.jnomics.ob.writable.NucleotideSequenceWritable#clear()
     */
    @Override
    public void clear() {
        super.clear();

        cigarString.clear();
        flags = UNDEFINED_INT;
        mappingQuality = UNDEFINED_INT;
        nextPosition = UNDEFINED_INT;
        nextReferenceName.clear();
        phredString.clear();
        properties.clear();
        queryTemplate = null;
    }

    /**
     * The default query template compare implementation. Simply returns the
     * value of <code>getReadNameText().compareTo(o.getReadNameText())</code>.
     * 
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    public int compareTo(SequencingRead o) {
        return getReadName().compareTo(o.getReadName());
    }

    /**
     * The CIGAR string, containing a list of CIGAR operations. See the <a
     * href="http://samtools.sourceforge.net/SAM1.pdf">the SAM format
     * specification</a> for more information.
     * 
     * @return The {@link Text} object containing the CIGAR string.
     */
    public Text getCigar() {
        return cigarString;
    }

    /**
     * Returns a SAM-style flags value, equivalent to the contents of column 2
     * of a SAM entry.
     */
    public int getFlags() {
        return flags;
    }

    /**
     * Returns the 1-based leftmost mapping position. It is identical to a call
     * to <code>getEndpoints().first()</code>.
     * 
     * @return The 1-based leftmost mapping position, or 0 if the sequence is
     *         unmapped.
     */
    public int getMappingPosition() {
        return getEndpoints().first();
    }

    /**
     * <p>
     * The fragment's overall mapping quality, equivalent to the contents of
     * column 5 of a SAM entry.
     * </p>
     * <p>
     * In the standard (BAM/SAM) implementation, this equals
     * -10log<sub>10</sub>Pr{mapping position is wrong}, rounded to the nearest
     * integer. A value of 255 indicates that the value is unavailable.
     * </p>
     * 
     * @return An unsigned byte value (0..255). A value of 255 indicates that
     *         the value is unavailable.
     */
    public int getMappingQuality() {
        return mappingQuality;
    }

    /**
     * The position of the next fragment in the template. Set as 0 by default,
     * or when the information is unavailable.
     */
    public int getNextPosition() {
        return nextPosition;
    }

    /**
     * The reference name of the next fragment in this template. Possible
     * special values are as follows:
     * <ul>
     * <li> <code>(empty string)</code> or <code>*</code> - Information is not
     * available.
     * <li> <code>=</code> - Value is identical to this fragment's reference name
     * ({@link SequencingRead#getReferenceName()})
     * </ul>
     */
    public Text getNextReferenceName() {
        return nextReferenceName;
    }

    /**
     * The ASCII-encoded quality string for this read, equivalent to the
     * contents of SAM column 11. Unlike a standard SAM record entry, however,
     * this isn't guaranteed to be in Sanger (Phred+33) format.
     * 
     * @return The {@link Text} object containing the ASCII-encoded quality
     *         string.
     */
    public Text getPhredString() {
        return phredString;
    }

    /**
     * Arbitrary properties. The implementation is dependent on the original
     * format. In a SAM record, for example, this would be where the optional
     * tags are placed.
     */
    public ReadProperties getProperties() {
        return properties;
    }

    /**
     * The query template that this read is a member of. Note that this value
     * doesn't get stored during a write() operation; rather, it gets
     * read/written during a {@link QueryTemplate} read/write.
     * 
     * @return The query template that contains this read; or <code>null</code>
     *         if this value hasn't been set.
     */
    public QueryTemplate getQueryTemplate() {
        return queryTemplate;
    }

    /**
     * The read's name (query template name), equivalent to the contents of SAM
     * column 1. An empty string indicates that the information is unavailable.
     * 
     * @return Returns the {@link Text} object underlying the read name. The
     *         result of <code>this.getReadNameText().toString()</code> is
     *         identical to <code>this.toString()</code>.
     */
    public Text getReadName() {
        return readName;
    }

    /**
     * Returns true if all of the specified flags are set.
     * 
     * @param flags One or more FLAG_* constants. A <code>0</code> value will
     *            always return <code>true</code>.
     */
    public boolean hasAllFlags(int flags) {
        return (flags == (flags & this.flags));
    }

    /**
     * Returns true if any of the specified flags are set.
     * 
     * @param flags One or more FLAG_* constants. A <code>0</code> value will
     *            always return <code>true</code>.
     */
    public boolean hasAnyFlags(int flags) {
        return ((0 == flags) || (0 != (flags & this.flags)));
    }

    /**
     * Returns <code>true</code> if the specified flag is set. This method
     * simply returns the value of {@link #hasAllFlags(int)}.
     * 
     * @param flag One or more FLAG_* constants. A <code>0</code> value will
     *            always return <code>true</code>.
     * @return <code>true</code> if the specified flag is set;
     *         <code>false</code> otherwise.
     */
    public boolean hasFlag(int flag) {
        return hasAllFlags(flag);
    }

    /**
     * Returns <code>true</code> if this sequence has been identified by the
     * aligner as a PCR or optical duplicate. This method is identical to
     * <code>{@link #hasFlag(int) hasFlag}({@link #FLAG_DUPLICATE})</code>.
     */
    public boolean isDuplicate() {
        return hasAllFlags(FLAG_DUPLICATE);
    }

    /**
     * Returns <code>true</code> if this sequence has been marked by the aligner
     * as failing quality controls. This method is identical to
     * <code>{@link #hasFlag(int) hasFlag}({@link #FLAG_SUBQUALITY})</code>.
     */
    public boolean isFailedQuality() {
        return hasAllFlags(FLAG_SUBQUALITY);
    }

    /**
     * Returns <code>true</code> if this sequence is the first segment in its
     * query template. This method is identical to
     * <code>{@link #hasFlag(int) hasFlag}({@link #FLAG_FIRST_SEGMENT})</code>.
     * <p>
     * If both {@link #isFirst()} and {@link #isLast()} are <code>true</code>
     * then this segment is part of a linear template is neither the first nor
     * the last segment. If both are <code>false</code> then the index of the
     * segment in the template is unknown, which may happen if the template is
     * non-linear if the index is lost during data processing.
     * <p>
     * If {@link #isTemplateMultiplySegmented()} is <code>false</code>, then no
     * assumptions may be made about this method.
     */
    public boolean isFirst() {
        return hasAllFlags(FLAG_FIRST_SEGMENT);
    }

    /**
     * Returns <code>true</code> if this sequence is the last segment in its
     * query template. This method is identical to
     * <code>{@link #hasFlag(int) hasFlag}({@link #FLAG_LAST_SEGMENT})</code>.
     * <p>
     * If {@link #isTemplateMultiplySegmented()} is <code>false</code>, then no
     * assumptions may be made about this method.
     */
    public boolean isLast() {
        return hasAllFlags(FLAG_LAST_SEGMENT);
    }

    /**
     * The read's name (query template name), equivalent to the contents of SAM
     * column 1. An empty string indicates that the information is unavailable.
     * 
     * @return Returns the {@link Text} object underlying the read name. The
     *         result of <code>this.getReadNameText().toString()</code> is
     *         identical to <code>this.toString()</code>.
     */

    /**
     * Returns <code>true</code> if this segment has been successfully mapped by
     * an aligner, or <code>false</code> if no mapping has been attempted or
     * mapping was not successful. This method is identical to
     * <code>!{@link #hasFlag(int) hasFlag}({@link #FLAG_UNMAPPED})</code>.
     * <p>
     * If this returns <code>false</code>, then no assumptions can be made about
     * the reference identity, mapping position or quality, CIGAR sequence, or
     * any property of the next sequence in the query template.
     */
    public boolean isMapped() {
        return !hasAllFlags(FLAG_UNMAPPED);
    }

    /**
     * Returns <code>true</code> if the next segment in the template has been
     * mapped by an aligner, or <code>false</code> if no mapping has been
     * attempted or mapping was not successful. This method is identical to
     * <code>!{@link #hasFlag(int) hasFlag}({@link #FLAG_NEXT_UNMAPPED})</code>
     * .
     * <p>
     * If this returns <code>false</code>, then no assumptions can be made about
     * the reference identity, mapping position or quality, CIGAR sequence, or
     * any property of the next sequence in the query template.
     * <p>
     * If {@link #isTemplateMultiplySegmented()} is <code>false</code>, then no
     * assumptions may be made about this method.
     */
    public boolean isNextMapped() {
        return !hasAllFlags(FLAG_NEXT_UNMAPPED);
    }

    /**
     * Returns <code>true</code> if the next sequence in the template has been
     * mapped to the reverse strand of the reference sequence. This method is
     * identical to
     * <code>{@link #hasFlag(int) hasFlag}({@link #FLAG_NEXT_REVERSE_COMPLEMENTED})</code>
     * .
     * <p>
     * If both {@link #isFirst()} and {@link #isLast()} are <code>true</code>
     * then this segment is part of a linear template is neither the first nor
     * the last segment. If both are <code>false</code> then the index of the
     * segment in the template is unknown; this may happen if the template is
     * non-linear if the index is lost during data processing.
     * <p>
     * If either {@link #isMapped()} or {@link #isTemplateMultiplySegmented()}
     * is <code>false</code>, then no assumptions may be made about this method.
     */
    public boolean isNextReverseComplemented() {
        return hasAllFlags(FLAG_NEXT_REVERSE_COMPLEMENTED);
    }

    /**
     * Returns <code>true</code> if this all segments within this read's
     * {@link QueryTemplate} are property aligned according to an aligner. This
     * method is identical to
     * <code>{@link #hasFlag(int) hasFlag}({@link #FLAG_EACH_PROPERLY_ALIGNED})</code>
     * .
     * <p>
     * If either {@link #isMapped()} or {@link #isTemplateMultiplySegmented()}
     * is <code>false</code>, then no assumptions may be made about this method.
     */
    public boolean isProperlyPaired() {
        return hasAllFlags(FLAG_EACH_PROPERLY_ALIGNED);
    }

    /**
     * Returns <code>true</code> if this sequence has been mapped to the reverse
     * strand of the reference sequence.
     * <code>{@link #hasFlag(int) hasFlag}({@link #FLAG_REVERSE_COMPLEMENTED})</code>
     * .
     * <p>
     * If either {@link #isMapped()} is <code>false</code>, then no assumptions
     * may be made about this method.
     */
    public boolean isReverseComplemented() {
        return hasAllFlags(FLAG_REVERSE_COMPLEMENTED);
    }

    /**
     * Returns <code>true</code> if the segments alignment values are the
     * product of a secondary alignment. This method is identical to
     * <code>{@link #hasFlag(int) hasFlag}({@link #FLAG_SECONDARY_ALIGNMENT})</code>
     * .
     * <p>
     * If {@link #isMapped()} is <code>false</code>, then no assumptions may be
     * made about this method.
     */
    public boolean isSecondaryAlignment() {
        return hasAllFlags(FLAG_SECONDARY_ALIGNMENT);
    }

    /**
     * Returns <code>true</code> if this read's {@link QueryTemplate} has (or
     * had) multiple segments in sequencing (as with paired reads, for example).
     * This method is identical to
     * <code>{@link #hasFlag(int) hasFlag}({@link #FLAG_MULTIPLE_FRAGMENTS})</code>
     * .
     */
    public boolean isTemplateMultiplySegmented() {
        return hasAllFlags(FLAG_MULTIPLE_FRAGMENTS);
    }

    /*
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        readName.readFields(in);
        flags = in.readUnsignedShort();
        nextReferenceName.readFields(in);
        super.readFields(in);
        mappingQuality = in.readUnsignedByte();
        cigarString.readFields(in);
        nextPosition = in.readInt();
        phredString.readFields(in);
        properties.readFields(in);

        // True if query template data follows; false otherwise.
        if (in.readBoolean()) {
            if (queryTemplate == null) {
                queryTemplate = new QueryTemplate();
            }

            queryTemplate.getTemplateName().readFields(in);
            queryTemplate.setTemplatePosition(in.readInt());
            queryTemplate.setTemplateLength(in.readInt());
        } else {
            queryTemplate = null;
        }
    }

    /**
     * Reverse complements this read, inverts the orientation, reverses the
     * order of the Phred string, and flips the value of the
     * "reverse complemented" flag.
     * 
     * @see edu.cshl.schatz.jnomics.ob.AbstractNucleotideSequence#reverseComplement()
     */
    @Override
    public void reverseComplement() {
        super.reverseComplement();

        flags ^= FLAG_REVERSE_COMPLEMENTED;

        // Perform an in-place reversal of the array contents, reverse
        // complementing in the process.

        byte bytes[] = getPhredString().getBytes();
        byte tmp;
        for (int pos5 = 0, pos3 = length() - 1; pos5 < pos3; pos5++, pos3--) {
            tmp = bytes[pos3];
            bytes[pos3] = bytes[pos5];
            bytes[pos5] = tmp;
        }
    }

    /*
     * @see
     * edu.cshl.schatz.jnomics.io.NucleotideSequence#set(edu.cshl.schatz.jnomics
     * .io.NucleotideSequence)
     */
    @Override
    public void set(NucleotideSequence sourceSequence) {
        super.set(sourceSequence);

        if (sourceSequence instanceof SequencingRead) {
            SequencingRead read = (SequencingRead) sourceSequence;

            cigarString.set(read.cigarString);
            flags = read.flags;
            mappingQuality = read.mappingQuality;
            nextPosition = read.nextPosition;
            nextReferenceName.set(read.nextReferenceName);
            phredString.set(read.phredString);
            properties.putAll(read.properties);
            queryTemplate = read.queryTemplate;
            readName.set(read.readName);
        }
    }

    /**
     * @param cigar The cigarString to set
     */
    public void setCigar(String cigar) {
        cigarString.set(cigar);
    }

    /**
     * @param cigar The cigarString to set
     */
    public void setCigar(Text cigar) {
        cigarString.set(cigar);
    }

    /**
     * @param flags The flags to set
     */
    public void setFlags(int flags) {
        this.flags = flags;
    }

    /**
     * Sets the fragment's overall mapping quality.
     */
    public void setMappingQuality(int mappingQuality) {
        this.mappingQuality = mappingQuality;
    }

    /**
     * @param nextPosition The nextPosition to set
     */
    public void setNextPosition(int nextPosition) {
        this.nextPosition = nextPosition;
    }

    /**
     * @param nextReferenceName The nextReferenceName to set
     */
    public void setNextReferenceName(String nextReferenceName) {
        this.nextReferenceName.set(nextReferenceName);
    }

    /**
     * @param nextReferenceName The nextReferenceName to set
     */
    public void setNextReferenceName(Text nextReferenceName) {
        this.nextReferenceName.set(nextReferenceName);
    }

    /**
     * @param phred The phredString to set
     */
    public void setPhred(String phred) {
        phredString.set(phred);
    }

    /**
     * @param phred The phredString to set
     */
    public void setPhred(Text phred) {
        phredString.set(phred);
    }

    /**
     * Arbitrary properties. The implementation is dependent on the original
     * format. In a SAM record, for example, this would be where the optional
     * tags are placed.
     */
    public void setProperties(MapWritable properties) {
        this.properties = new ReadProperties(properties);
    }

    /**
     * @param readName The queryTemplateNameString to set
     */
    public void setReadName(String readName) {
        this.readName.set(readName);
    }

    /**
     * @param readName The queryTemplateNameString to set
     */
    public void setReadName(Text readName) {
        this.readName.set(readName);
    }

    /**
     * @param referenceSequence The referenceNameString to set
     */
    public void setReferenceSequence(ReferenceSequence referenceSequence) {
        throw new UnsupportedOperationException();
    }

    /**
     * <p>
     * This method is designed to emulate the <code>fastq_quality_trimmer</code>
     * (FASTX-toolkit, Assaf Gordon, <a
     * href="http://hannonlab.cshl.edu/fastx_toolkit/" />. From that tool's
     * documentation:
     * </p>
     * 
     * <pre>
     * **What it does: Trims the sequence according to the specified quality
     * threshold. This tool scans the sequence from the end for the
     * first nucleotide to possess the specified minimum quality score. It
     * will then trim (remove nucleotides from) the sequence after this
     * position. After trimming, sequences that are shorter than the minimum
     * length are discarded.
     * 
     * Example** Input Fasta file (with 20 bases in each sequences)::
     * 
     *     &amp;1
     *     TATGGTCAGAAACCATATGC
     *     +1
     *     40 40 40 40 40 40 40 40 40 40 40 20 19 19 19 19 19 19 19 19
     *     &amp;2
     *     CAGCGAGGCTTTAATGCCAT
     *     +2
     *     40 40 40 40 40 40 40 40 30 20 19 20 19 19 19 19 19 19 19 19
     *     &amp;3
     *     CAGCGAGGCTTTAATGCCAT
     *     +3
     *     40 40 40 40 40 40 40 40 20 19 19 19 19 19 19 19 19 19 19 19
     * 
     * 
     * Trimming with a cutoff of 20, we get the following FASTQ file::
     * 
     *     &amp;1
     *     TATGGTCAGAAA
     *     +1
     *     40 40 40 40 40 40 40 40 40 40 40 20
     *     &amp;2
     *     CAGCGAGGCTTT
     *     +2
     *     40 40 40 40 40 40 40 40 30 20 19 20
     *     &amp;3
     *     CAGCGAGGC
     *     +3
     *     40 40 40 40 40 40 40 40 20
     *     
     * 
     * Trimming with a cutoff of 20 and a minimum length of 12, we get the
     * following FASTQ file::
     * 
     *     &amp;1
     *     TATGGTCAGAAA
     *     +1
     *     40 40 40 40 40 40 40 40 40 40 40 20
     *     &amp;2
     *     CAGCGAGGCTTT
     *     +2
     *     40 40 40 40 40 40 40 40 30 20 19 20
     * </pre>
     * 
     * @param threshold Phred quality threshold - nucleotides with lower quality
     *            will be trimmed from the end of the sequence.
     * @param offset Handle SAM ASCII quality with the specified offset. Usually
     *            64, but for Sanger reads this should be set to 33.
     * @return The number of nucleotides trimmed (0 = no change).
     */
    public int trimByQuality(int threshold, int offset) {
        byte[] phreds = getPhredString().getBytes();
        int length = length();

        // If the 3' terminal base is above our threshold, don't waste time
        // on an expensive substring; just bail. Otherwise, keep on keepin'
        // on, man.

        if ((length > 0) && ((phreds[length - 1] - offset) < threshold)) {
            for (int i = length - 2; i >= 0; i--) {
                if ((phreds[i] - offset) >= threshold) {
                    subSequence(this, getEndpoints().first(), getEndpoints().first() + i);

                    return length - i;
                }
            }
        }

        return 0;
    }

    /**
     * Serialize the fields of this object to <code>out</code>.
     * 
     * @param out <code>DataOuput</code> to serialize this object into.
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        write(out, true);
    }

    /**
     * The query template that this read is a member of. Note that this value
     * doesn't get stored during a write() operation; rather, it gets
     * read/written only by a QueryTemplate read/write.
     */
    protected void setQueryTemplate(QueryTemplate queryTemplate) {
        this.queryTemplate = queryTemplate;
    }

    /**
     * If this is being written as part of a {@link QueryTemplate}
     * <tt>write</tt> call, then we don't want to save any template data since
     * the {@link QueryTemplate#write(DataOutput)} method is already handling
     * that. If, however, we're just writing a {@link SequencingRead} alone,
     * then we want to store some basic information about the
     * <tt>QueryTemplate</tt> so that it can be restored after the fact.
     * 
     * @param writeTemplateData <code>True</code> to write the name, length, and
     *            position of the read's {@link QueryTemplate} to <tt>out</tt>;
     *            <code>false</code> otherwise.
     */
    protected void write(DataOutput out, boolean writeTemplateData) throws IOException {
        readName.write(out);
        out.writeShort(flags);
        nextReferenceName.write(out);
        super.write(out);
        out.writeByte(mappingQuality);
        cigarString.write(out);
        out.writeInt(nextPosition);
        phredString.write(out);
        properties.write(out);

        // The next bit indicates whether query template data follows.
        if (!writeTemplateData || queryTemplate == null) {
            out.writeBoolean(false);
        } else if (writeTemplateData) {
            out.writeBoolean(true);
            queryTemplate.getTemplateName().write(out);
            out.writeInt(queryTemplate.getTemplatePosition());
            out.writeInt(queryTemplate.getTemplateLength());
        }
    }

    /**
     * TODO Not yet in use; to be implemented with addition of total support for
     * SAM-style headers.
     */
    public class ProgramRecord {}

    /**
     * TODO Not yet in use; to be implemented with addition of total support for
     * SAM-style headers.
     */
    public class ReadGroup {}

    /**
     * An extension of Hadoop's {@link MapWritable} class, optimized slightly to
     * handle key {@link String Strings} in a more user-friendly manner by
     * automatically converting them to {@link Text}.
     * 
     * @author Matthew A. Titmus
     */
    public static class ReadProperties extends MapWritable {
        private Text textKey = new Text();

        /**
         * Default constructor.
         */
        public ReadProperties() {
            super();
        }

        /**
         * Copy constructor.
         * 
         * @param other the map to copy from
         */
        public ReadProperties(MapWritable other) {
            super(other);
        }

        /*
         * @see org.apache.hadoop.io.MapWritable#containsKey(java.lang.Object)
         */
        public boolean containsKey(byte[] utf8) {
            textKey.set(utf8);
            return super.containsKey(textKey);
        }

        /*
         * @see org.apache.hadoop.io.MapWritable#containsKey(java.lang.Object)
         */
        public boolean containsKey(String key) {
            textKey.set(key);
            return super.containsKey(textKey);
        }

        /*
         * @see org.apache.hadoop.io.MapWritable#containsValue(java.lang.Object)
         */
        @Override
        public boolean containsValue(Object value) {
            return super.containsValue(value);
        }

        /*
         * @see java.lang.Object#equals(java.lang.Object)
         */
        @Override
        public boolean equals(Object obj) {
            if (MapWritable.class.isAssignableFrom(obj.getClass())) {
                MapWritable map = (MapWritable) obj;

                if (map.size() == size()) {
                    Writable objKey, objValue, myValue;

                    for (Entry<Writable, Writable> e : map.entrySet()) {
                        objKey = e.getKey();
                        objValue = e.getValue();

                        if ((null == (myValue = this.get(objKey))) || !objValue.equals(myValue)) {
                            return false;
                        }
                    }

                    return true;
                }
            }

            return false;
        }

        /*
         * @see org.apache.hadoop.io.MapWritable#get(java.lang.Object)
         */
        public Writable get(byte[] utf8) {
            textKey.set(utf8);
            return super.get(textKey);
        }

        /*
         * @see org.apache.hadoop.io.MapWritable#get(java.lang.Object)
         */
        public Writable get(String key) {
            textKey.set(key);
            return super.get(textKey);
        }

        /*
         * @see
         * org.apache.hadoop.io.MapWritable#put(org.apache.hadoop.io.Writable,
         * org.apache.hadoop.io.Writable)
         */
        public Writable put(byte[] utf8, Writable value) {
            return super.put(new Text(utf8), value);
        }

        /*
         * @see
         * org.apache.hadoop.io.MapWritable#put(org.apache.hadoop.io.Writable,
         * org.apache.hadoop.io.Writable)
         */
        public Writable put(String key, String value) {
            return super.put(new Text(key), new Text(value));
        }

        /*
         * @see
         * org.apache.hadoop.io.MapWritable#put(org.apache.hadoop.io.Writable,
         * org.apache.hadoop.io.Writable)
         */
        public Writable put(String key, Writable value) {
            return super.put(new Text(key), value);
        }

        /*
         * @see org.apache.hadoop.io.MapWritable#remove(java.lang.Object)
         */
        public Writable remove(byte[] utf8) {
            textKey.set(utf8);
            return super.remove(textKey);
        }

        /*
         * @see org.apache.hadoop.io.MapWritable#remove(java.lang.Object)
         */
        public Writable remove(String key) {
            textKey.set(key);
            return super.remove(textKey);
        }

        /*
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            Writable keys[] = keySet().toArray(new Writable[0]), key, value;
            StringBuilder sb = new StringBuilder("{");

            Arrays.sort(keys);

            if (keys.length > 0) {
                key = keys[0];
                value = get(key);

                if (key instanceof Text) {
                    sb.append("\"" + key.toString() + "\":");
                } else {
                    sb.append(key.toString() + ":");
                }

                if (value == null) {
                    sb.append("null");
                } else if (value instanceof Text) {
                    sb.append("\"" + value.toString() + "\"");
                } else {
                    sb.append(value.toString());
                }
            }

            for (int i = 1; i < keys.length; i++) {
                key = keys[i];
                value = get(key);

                if (key instanceof Text) {
                    sb.append(", \"" + key.toString() + "\":");
                } else {
                    sb.append(", " + key.toString() + ":");
                }

                if (value == null) {
                    sb.append("null");
                } else if (value instanceof Text) {
                    sb.append("\"" + value.toString() + "\"");
                } else {
                    sb.append(value.toString());
                }
            }

            sb.append('}');

            return sb.toString();
        }
    }

    /**
     * Basic information about the reference sequence used to generate an
     * aligned {@link SequencingRead}.
     * <p>
     * TODO Not yet in use; to be implemented with addition of total support for
     * SAM-style headers.
     * 
     * @author Matthew Titmus
     */
    public static class ReferenceSequence implements Writable {
        private String assemblyIdentifier = "";

        private int length;

        private String md5 = "";

        private String name = "";

        private String species = "";

        private String uri = "";

        /**
         * The basic constructor, which accepts only the required fields.
         * 
         * @param sequenceName
         * @param sequenceLength
         */
        public ReferenceSequence(String sequenceName, int sequenceLength) {
            this(sequenceName, sequenceLength, "", "", "", "");
        }

        /**
         * A more elaborate constructor, which accepts all fields. Optional
         * fields (anything except <code>sequenceName</code> and
         * <code>sequenceLength</code>) may be passed <code>null</code> or an
         * empty string ("").
         * 
         * @param sequenceName The reference sequence name. Required.
         * @param sequenceLength The reference sequence length. Required
         *            (greater than 0).
         * @param genomeAssemblyIdentifier The genome assembly identifier.
         * @param genomeSpecies
         * @param sequenceMD5Checksum The md5 checksum of the sequence.
         * @param sequenceURI URI of the sequence. If it does not start with a
         *            standard schema (http, ftp, etc) it will be assumed to
         *            refer to a filesystem path.
         */
        public ReferenceSequence(String sequenceName, int sequenceLength,
            String genomeAssemblyIdentifier, String genomeSpecies, String sequenceMD5Checksum,
            String sequenceURI) {

            if ((sequenceName == null) || (sequenceName.length() == 0)) {
                throw new IllegalArgumentException("Sequence name is required.");
            } else {
                name = sequenceName;
            }

            if (sequenceLength <= 0) {
                throw new IllegalArgumentException("Sequence length is required.");
            } else {
                length = sequenceLength;
            }

            if (assemblyIdentifier == null) {
                genomeAssemblyIdentifier = "";
            }

            if (genomeSpecies == null) {
                genomeSpecies = "";
            }

            if (sequenceURI == null) {
                sequenceURI = "";
            } else {
                if (!sequenceURI.matches("^\\w+:.*")) {
                    sequenceURI = "file://" + sequenceURI;
                }
            }

            if (sequenceMD5Checksum == null) {
                sequenceMD5Checksum = "";
            } else {
                sequenceMD5Checksum = sequenceMD5Checksum.toUpperCase().replaceAll("[\\s]+", "");
            }

            assemblyIdentifier = genomeAssemblyIdentifier;
            species = genomeSpecies;
            md5 = sequenceMD5Checksum;
            uri = sequenceURI;
        }

        /**
         * Creates an empty reference sequence.
         */
        ReferenceSequence() {}

        /**
         * @return The identifier
         */
        public String getAssemblyIdentifier() {
            return assemblyIdentifier;
        }

        /**
         * @return The length
         */
        public int getLength() {
            return length;
        }

        /**
         * @return The mD5
         */
        public String getMD5() {
            return md5;
        }

        /**
         * @return The name
         */
        public String getName() {
            return name;
        }

        /**
         * @return The species
         */
        public String getSpecies() {
            return species;
        }

        /**
         * @return The URI
         */
        public String getURI() {
            return uri;
        }

        /*
         * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
         */
        public void readFields(DataInput in) throws IOException {
            // TODO
            throw new UnsupportedOperationException();
        }

        /*
         * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
         */
        public void write(DataOutput out) throws IOException {
            // TODO
            throw new UnsupportedOperationException();
        }
    }
}
