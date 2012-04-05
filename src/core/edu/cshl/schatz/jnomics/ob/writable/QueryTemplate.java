/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.ob.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import edu.cshl.schatz.jnomics.ob.PositionRange;

/**
 * Represents a DNA/RNA sequence, some or all of which is sequenced on a
 * sequencing machine or assembled from raw sequences. Each
 * {@link QueryTemplate} generally contains one or more {@link SequencingRead}
 * instances, each of which represents a raw sequence that came off a sequencing
 * machine. These reads may be accessed and manipulated via the methods defined
 * in the {@link List} interface.
 * <p>
 * The {@link QueryTemplate} and {@link SequencingRead} are based on the
 * "template" and "read", as defined in <a
 * href="http://samtools.sourceforge.net/SAM1.pdf">The SAM Format
 * Specification</a> version 1.4-r962.
 * <p>
 * {@link QueryTemplate} complies with the Jnomics specification that when
 * {@link Writable Writables} are added to collections, the collection SHOULD
 * store a copy of the <code>Writable</code>, rather than a reference to the
 * original object.
 * <p>
 * TODO: Implement an object pool of some kind to minimize the number of new
 * {@link SequencingRead} instances created during set operations.
 * </p>
 * 
 * @see SequencingRead
 * @author Matthew Titmus
 */
public class QueryTemplate extends AbstractList<SequencingRead>
        implements List<SequencingRead>, WritableComparable<QueryTemplate> {

    private static final SequencingRead[] EMPTY_POOL = new SequencingRead[] {};

    /**
     * Used to quickly clear the values array. Since
     * {@link System#arraycopy(Object, int, Object, int, int)} is implemented at
     * the operating system level, it is inherently faster than a <i>for</i>
     * loop for this purpose.
     */
    private static SequencingRead[] nullPool = new SequencingRead[10];

    /**
     * The object pool for sequencing reads. Elements in this pool are not
     * cleared returned to the pool and may contain values set during a
     * "previous life".
     */
    private SequencingRead[] readPool = EMPTY_POOL;

    /**
     * Points to the highest unoccupied position in the read pool. For an empty
     * pool this is 0, and is incremented each time a read is placed into the
     * pool.
     */
    private int readPoolPointer = 0;

    /**
     * The internal reads array. Only indices < size() are meaningful.
     */
    private SequencingRead[] readsArray = new SequencingRead[3];

    /**
     * The number of reads that compose this query template.
     */
    private int size = 0;

    /**
     * SAM column 9: The mapping distance between the leftmost mapped base of
     * the first read and the rightmost mapped base of the last read (including
     * the bases in the gap between the reads). It does not reflect any changes
     * to the sequence length as a result of trimming or other modifications.
     */
    private int templateLength = 0;

    private Text templateName = new Text("");

    /**
     * Sets the 1-based mapping position of the leftmost read associated with
     * this query template.
     */
    private int templatePosition = 0;

    /**
     * Creates an new, empty QueryTemplate instance.
     */
    public QueryTemplate() {}

    /**
     * Creates a new QueryTemplate instance that is an exact copy of the
     * <code>toCopy</code> parameter.
     */
    public QueryTemplate(QueryTemplate toCopy) {
        set(toCopy);
    }

    /**
     * Inserts the specified {@link SequencingRead} at the specified position.
     * Shifts the element currently at that position (if any) and any subsequent
     * elements to the right (adds one to their indices).
     * 
     * @param index The index at which the read is to be inserted
     * @param read {@link SequencingRead} instance to be to be inserted
     * @throws NullPointerException if the specified element is
     *             <code>null</code>
     * @throws IndexOutOfBoundsException if the index is out of range (
     *             <tt>index &lt; 0 || index &gt; size()</tt>)
     */
    @Override
    public void add(int index, SequencingRead read) {
        ensureCapacity(size + 1);

        System.arraycopy(readsArray, index, readsArray, index + 1, size - index);

        readsArray[index] = getEmptyRead();
        readsArray[index].set(read);
        readsArray[index].setQueryTemplate(this);

        size++;
    }

    /**
     * Appends the specified {@link SequencingRead} to the end of the template.
     * 
     * @param read the sequencing read to be appended to this list
     * @return <tt>true</tt> (as specified by {@link Collection#add})
     * @throws NullPointerException if the specified element is
     *             <code>null</code>.
     */
    @Override
    public boolean add(SequencingRead read) {
        ensureCapacity(size + 1);

        SequencingRead add = getEmptyRead();
        add.set(read);
        add.setQueryTemplate(this);

        readsArray[size++] = add;

        return true;
    }

    /**
     * Appends all of the reads in the specified collection to the end of this
     * template, in the order that they are returned by the specified
     * collection's iterator. The behavior of this operation is undefined if the
     * specified collection is modified while the operation is in progress (this
     * will occur if the specified collection is this template, and it's
     * nonempty).
     * 
     * @param c collection containing reads to be added to this template.
     * @return <tt>true</tt> if this template changed as a result of the call
     * @throws ClassCastException if the class of an read of the specified
     *             collection prevents it from being added to this template.
     * @throws NullPointerException if the specified collection contains one or
     *             more {@link NullPointerException} reads or if the specified
     *             collection is <code>null</code>.
     * @throws IllegalArgumentException if some property of an read of the
     *             specified collection prevents it from being added to this
     *             template.
     * @see #add(Object)
     */
    @Override
    public boolean addAll(Collection<? extends SequencingRead> c) {
        Object[] a = c.toArray();
        int numNew = a.length;

        SequencingRead r;
        for (Object element : a) {
            r = getPooledRead();
            r.set((SequencingRead) element);
            r.setQueryTemplate(this);
        }

        ensureCapacity(size + numNew); // Increments modCount
        System.arraycopy(a, 0, readsArray, size, numNew);

        size += numNew;

        return numNew != 0;
    }

    /**
     * Inserts all of the reads in the specified collection into this template
     * at the specified position. Shifts the read currently at that position (if
     * any) and any subsequent reads to the right (increases their indices). The
     * new reads will appear in this template in the order that they are
     * returned by the specified collection's iterator. The behavior of this
     * operation is undefined if the specified collection is modified while the
     * operation is in progress (this will occur if the specified collection is
     * this template, and it's nonempty).
     * 
     * @param index index at which to insert the first read from the specified
     *            collection
     * @param c collection containing reads to be added to this template.
     * @return <tt>true</tt> if this template changed as a result of the call
     * @throws ClassCastException if the class of an read of the specified
     *             collection prevents it from being added to this template.
     * @throws NullPointerException if the specified collection contains one or
     *             more <code>null</code> reads.
     * @throws IllegalArgumentException if some property of an read of the
     *             specified collection prevents it from being added to this
     *             template.
     * @throws IndexOutOfBoundsException if the index is out of range (
     *             <tt>index &lt; 0 || index &gt; size()</tt>).
     */
    @Override
    public boolean addAll(int index, Collection<? extends SequencingRead> c) {
        checkBounds(index);

        SequencingRead[] a = c.toArray(new SequencingRead[] {});
        int numNew = a.length;

        SequencingRead r;
        for (SequencingRead element : a) {
            r = getPooledRead();
            r.set(element);
            r.setQueryTemplate(this);
        }

        ensureCapacity(size + numNew); // Increments modCount

        int numMoved = size - index;

        if (numMoved > 0) {
            System.arraycopy(readsArray, index, readsArray, index + numNew, numMoved);
        }

        System.arraycopy(a, 0, readsArray, index, numNew);

        size += numNew;

        return numNew != 0;
    }

    /**
     * When called, this method calculates a template length and position based
     * on the first and last positions of the reads within it. It <i>does
     * not</i> alter any existing values of this template.
     */
    public PositionRange calculateTemplatePosition() {
        int first = 0, last = 0, length = 0;

        for (SequencingRead sr : this) {
            int newFirst = sr.getEndpoints().first();
            int newLast = sr.getEndpoints().last();

            if ((newFirst > 0) && ((newFirst < first) || (first == 0))) {
                first = newFirst;
            }

            if ((newLast > 0) && ((newLast > last) || (last == 0))) {
                last = newLast;
            }
        }

        if ((first != 0) && (last != 0)) {
            length = (last - first) + 1;
        }

        return PositionRange.instanceByLength(first, length);
    }

    /**
     * Removes all of the reads from the template and resets all properties to
     * their default values. The template will be empty after this call returns.
     */
    @Override
    public void clear() {
        modCount++;

        System.arraycopy(nullPool, 0, readsArray, 0, size);
        size = 0;
        templateLength = templatePosition = 0;
        templateName.clear();
    }

    /**
     * The default query template compare implementation; it only compares the
     * values of the template name.
     * 
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    public int compareTo(QueryTemplate o) {
        return getTemplateName().compareTo(o.getTemplateName());
    }

    /**
     * Returns <tt>true</tt> if this {@link QueryTemplate} contains the
     * specified element (assumed to be a {@link SequencingRead} instance). More
     * formally, it returns <tt>true</tt> if and only if this template contains
     * at least one SequencingRead <tt>r</tt> such that
     * <tt>((SequencingRead)&nbsp;o).equals(r)</tt>.
     * 
     * @param o element whose presence in this template is to be tested
     * @return <tt>true</tt> if this template contains the specified element
     * @throws ClassCastException if the type of the specified element is not an
     *             instance of {@link SequencingRead} (or subtype).
     * @throws NullPointerException if the specified element is
     *             <code>null</code>.
     */
    @Override
    public boolean contains(Object o) {
        return indexOf(o) >= 0;
    }

    /**
     * Compares the specified object with this list for equality. Returns
     * <tt>true</tt> if and only if the specified object is also a
     * {@link QueryTemplate}, both lists have the same number of reads, and all
     * corresponding pairs of elements in the two lists are <i>equal</i>. (Two
     * elements <tt>e1</tt> and <tt>e2</tt> are <i>equal</i> if
     * <tt>(e1==null ? e2==null : e1.equals(e2))</tt>.) In other words, two
     * templates are defined to be equal if they contain the same elements in
     * the same order.
     * 
     * @param obj the object to be compared for equality with this template.
     * @return <tt>true</tt> if the specified object is equal to this template.
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof QueryTemplate) {
            QueryTemplate qt = (QueryTemplate) obj;

            if ((size != qt.size) || (templateLength != qt.templateLength)
                    || (templateName != qt.templateName)) {

                return false;
            } else {
                for (int i = 0; i < size; i++) {
                    if (!qt.readsArray[i].equals(readsArray[i])) {
                        return false;
                    }
                }

                return true;
            }
        }

        return false;
    }

    /**
     * Returns the read at the specified position.
     * 
     * @param index index of the read to return
     * @return the read at the specified position in this list
     * @throws IndexOutOfBoundsException if the index is out of range (
     *             <tt>index &lt; 0 || index &gt;= size()</tt>)
     */
    @Override
    public SequencingRead get(int index) {
        checkBounds(index);

        return readsArray[index];
    }

    /**
     * Returns an empty {@link SequencingRead}. If a read already exists in the
     * reads array at position >= size, this returns it in its current
     * condition. Otherwise, a new instance will be created and placed at
     * position <code>size</code>.
     * <p>
     * Even if the read instance already exists in the internal array, it must
     * still be added via the add() method.
     */
    public SequencingRead getEmptyRead() {
        ensureCapacity(size + 1);

        return getPooledRead();
    }

    /**
     * If this query template is properly paired, then this returns the first
     * segment in this query template. The segment's value for
     * {@link SequencingRead#isProperlyPaired() isProperlyPaired()} and
     * {@link SequencingRead#isFirst() isFirst()} should be <code>true</code>,
     * and {@link SequencingRead#isLast() isLast()} should be <code>false</code>
     * .
     * 
     * @return The first segment in the template, or <code>null</code> if no
     *         segment is has the properly paired and first flags set and last
     *         flag unset.
     */
    public SequencingRead getFirst() {
        // TODO This is kludgey; fix it.

        for (int i = 0; i < size; i++) {
            if (readsArray[i].isProperlyPaired() && readsArray[i].isFirst()
                    && !readsArray[i].isLast()) {

                return readsArray[i];
            }
        }

        return null;
    }

    /**
     * If this query template is properly paired, then this returns the last
     * segment in this query template. The segment's value for
     * {@link SequencingRead#isProperlyPaired() isProperlyPaired()} and
     * {@link SequencingRead#isLast() isLast()} should be <code>true</code> ,
     * and {@link SequencingRead#isFirst() isFirst()} should be
     * <code>false</code>.
     * 
     * @return The last segment in the template, or <code>null</code> if no
     *         segment is has the properly paired and last flags set and first
     *         flag unset.
     */
    public SequencingRead getLast() {
        // TODO This is kludgey; fix it.

        for (int i = 0; i < size; i++) {
            if (readsArray[i].isProperlyPaired() && readsArray[i].isLast()
                    && !readsArray[i].isFirst()) {

                return readsArray[i];
            }
        }

        return null;
    }

    /**
     * Returns the underlying reads array (not a copy!). IMPORTANT: The array
     * returned may have a size larger than the value returned by the
     * <code>size()</code> method, and only indices <code>0</code> to
     * <code>size() - 1</code> are guaranteed to be both non- <code>null</code>
     * and contain the expected data. </p>
     * <p>
     * This method is available for speed, since looping over this list directly
     * is somewhat faster than using the <code>iterator()</code> or
     * <code>listIterator()</code> methods. Use caution, however, since
     * modifying its contents directly can have unexpected results.
     */
    public SequencingRead[] getReadsArray() {
        return readsArray;
    }

    /**
     * Gets the "observed" template length value, defined as the unsigned
     * mapping distance between the leftmost rightmost mapped bases, including
     * any gaps that may be present within or between reads and equivalent to
     * the (unsigned) value of column 9 of a SAM record. It is related to, but
     * is not necessarily the same as, the difference between the left-most
     * position of the left-most read and the right-most position of the
     * rightmost read.
     * <p>
     * If the observed template length is unknown or unspecified (i.e., it
     * returns <tt>0</tt>), the template length may estimated via the
     * {@link #calculateTemplatePosition()} method.
     * 
     * @see #calculateTemplatePosition()
     * @return An unsigned integer; a value of <tt>0</tt> indicated that the
     *         value is unset or that the {@link QueryTemplate} contains no
     *         mapped reads.
     */
    public int getTemplateLength() {
        return templateLength;
    }

    /**
     * The query template's name.
     * <p>
     * It is recommended that modifications to this value be done via the
     * {@link #setTemplateName(Text)} mutator method, rather than to the
     * {@link Text} instance returned by this method.
     * 
     * @see #getTemplateNameString()
     * @return The {@link Text} instance (not a copy); if undefined, the
     *         returned value will have a <tt>0</tt> length.
     */
    public Text getTemplateName() {
        return templateName;
    }

    /**
     * The query template's name.
     * 
     * @see #getTemplateName()
     * @return A string; if undefined, the returned value will have a <tt>0</tt>
     *         length.
     */
    public String getTemplateNameString() {
        return templateName.toString();
    }

    /**
     * Returns the 1-based mapping position of the leftmost read associated with
     * this query template. Unmapped reads are ignored.
     * 
     * @return A positive integer, or 0 if the template has no mapped reads.
     */
    public int getTemplatePosition() {
        return templatePosition;
    }

    /**
     * Returns the index of the first occurrence of the specified element in
     * this list, or <tt>-1</tt> if this list does not contain the element. More
     * formally, returns the lowest index <tt>i</tt> such that
     * <tt>(o==null&nbsp;?&nbsp;get(i)==null&nbsp;:&nbsp;o.equals(get(i)))</tt>,
     * or -1 if there is no such index.
     * 
     * @param o element to search for
     * @return the index of the first occurrence of the specified element in
     *         this list, or -1 if this list does not contain the element
     * @throws ClassCastException if the type of the specified element is
     *             incompatible with this list (optional)
     * @throws NullPointerException if the specified element is
     *             <code>null</code>.
     */
    @Override
    public int indexOf(Object o) {
        SequencingRead read = (SequencingRead) o;

        for (int i = 0; i < size; i--) {
            if (read.equals(readsArray[i])) {
                return i;
            }
        }

        return -1;
    }

    /**
     * Returns <tt>true</tt> if this list contains no elements.
     * 
     * @return <tt>true</tt> if this list contains no elements
     */
    @Override
    public boolean isEmpty() {
        return (size == 0);
    }

    /**
     * Returns the index of the last occurrence of the specified element in this
     * list, or -1 if this list does not contain the element. More formally,
     * returns the highest index <tt>i</tt> such that
     * <tt>(o==null&nbsp;?&nbsp;get(i)==null&nbsp;:&nbsp;o.equals(get(i)))</tt>,
     * or -1 if there is no such index.
     * 
     * @param o element to search for
     * @return the index of the last occurrence of the specified element in this
     *         list, or -1 if this list does not contain the element
     * @throws ClassCastException if the type of the specified element is
     *             incompatible with this list (optional)
     * @throws NullPointerException if the specified element is
     *             <code>null</code>.
     */
    @Override
    public int lastIndexOf(Object o) {
        SequencingRead read = (SequencingRead) o;

        for (int i = size - 1; i >= 0; i--) {
            if (read.equals(readsArray[i])) {
                return i;
            }
        }

        return -1;
    }

    /**
     * Deserialize the fields of this object from <code>in</code>.
     * 
     * @param in <code>DataInput</code> to deserialize this object from.
     * @throws IOException if thrown by the underlying input stream.
     */
    public void readFields(DataInput in) throws IOException {
        SequencingRead sr;
        int readCount = in.readInt();
        //System.out.println(readCount = in.readInt());
        ensureCapacity(readCount);

        for (int i = 0; i < readCount; i++) {
            if (null == (sr = readsArray[i])) {
                sr = readsArray[i] = getPooledRead();
            }

            sr.readFields(in);
            sr.setQueryTemplate(this);
        }

        this.size = readCount;
        templateName.readFields(in);
        templateLength = in.readInt();
        templatePosition = in.readInt();
    }

    /**
     * Removes the element at the specified position in this list. Shifts any
     * subsequent elements to the left (subtracts one from their indices).
     * Returns the element that was removed from the list.
     * <p>
     * The removed element is both returned <i>and</i> placed into the read
     * pool; retaining it may lead to unexpected results.
     * 
     * @param index the index of the element to be removed.
     * @return the element previously at the specified position
     * @throws IndexOutOfBoundsException if the index is out of range (
     *             <tt>index &lt; 0 || index &gt;= size()</tt>)
     */
    @Override
    public SequencingRead remove(int index) {
        if (index >= size) {
            throw new IndexOutOfBoundsException("Query=" + index + ", size=" + size);
        }

        modCount++;

        SequencingRead removed = readsArray[index];

        releaseRead(index);

        int numMoved = size - index - 1;
        if (numMoved > 0) {
            System.arraycopy(readsArray, index + 1, readsArray, index, numMoved);
        }

        releaseRead(--size);
        // readsArray[--size] = null;
        removed.setQueryTemplate(null);

        return removed;
    }

    /**
     * Removes the first occurrence of the specified element from this list, if
     * it is present. If this list does not contain the element, it is
     * unchanged. More formally, removes the element with the lowest index
     * <tt>i</tt> such that
     * <tt>(o==null&nbsp;?&nbsp;get(i)==null&nbsp;:&nbsp;o.equals(get(i)))</tt>
     * (if such an element exists). Returns <tt>true</tt> if this list contained
     * the specified element (or equivalently, if this list changed as a result
     * of the call).
     * 
     * @param o element to be removed from this list, if present
     * @return <tt>true</tt> if this list contained the specified element
     * @throws ClassCastException if the specified element is not an instance of
     *             {@link SequencingRead}.
     * @throws NullPointerException if the specified element is
     *             <code>null</code>.
     */
    @Override
    public boolean remove(Object o) {
        SequencingRead r = (SequencingRead) o;

        if (r == null) {
            throw new NullPointerException("QueryTemplate does not permit null elements");
        }

        for (int index = 0; index < size; index++) {
            if (r.equals(readsArray[index])) {
                releaseRead(index);
                fastRemove(index);
                r.setQueryTemplate(null);

                return true;
            }
        }

        return false;
    }

    /**
     * Replaces the read at the specified position with the specified read.
     * Unlike the standard {@link List} implementation, this value may not be
     * <code>null</code>.
     * 
     * @param index index of the element to replace.
     * @param read sequencing read to be stored at the specified position.
     * @return the read added (a copy of the parameter read).
     * @throws IndexOutOfBoundsException {@inheritDoc}
     * @throws NullPointerException if read value is <code>null</code>.
     */
    @Override
    public SequencingRead set(int index, SequencingRead read) {
        if (read != null) {
            checkBounds(index);

            SequencingRead toSet;

            if (null == (toSet = readsArray[index])) {
                toSet = readsArray[index] = getPooledRead();
            }

            toSet.set(read);
            toSet.setQueryTemplate(this);

            return toSet;
        } else {
            throw new NullPointerException("Cannot add a null read parameter to a query template.");
        }
    }

    /**
     * Sets this {@link QueryTemplate} to be an exact copy of the specified
     * instance (almost exact: mod count is tracked separately).
     * 
     * @param toCopy The QueryTemplate instance to copy.
     * @throws NullPointerException if the specified element is
     *             <code>null</code>.
     */
    public void set(QueryTemplate toCopy) {
        set(toCopy.readsArray);

        size = toCopy.size;
        templateLength = toCopy.templateLength;
        templatePosition = toCopy.templatePosition;
        templateName.set(toCopy.templateName);
    }

    /**
     * Replaces any reads contained within this query template and updates the
     * read count. The {@link SequencingRead#getQueryTemplate() queryTemplate}
     * property of each read is updated to point to this template.
     */
    public void set(SequencingRead... sequencingReads) {
        ensureCapacity(sequencingReads.length);
        size = sequencingReads.length;

        SequencingRead read;

        int ra, sr;
        for (ra = sr = 0; sr < sequencingReads.length; sr++) {
            if (null == sequencingReads[sr]) {
                continue;
            }

            if (null == (read = readsArray[ra])) {
                read = readsArray[ra] = getPooledRead();
            }

            read.set(sequencingReads[sr]);
            read.setQueryTemplate(this);
            ra++;
        }

        this.size = ra;
    }

    /**
     * Sets the "observed" template length value, defined as the mapping
     * distance between the leftmost rightmost mapped bases, including any gap
     * that may be present between reads (equivalent to the value of column 9 of
     * a SAM record, except that it is always positive). This value is related
     * to, but is not necessarily the same as, the difference between the
     * left-most position of the left-most read and the right-most position of
     * the rightmost read.
     * <p>
     * If the observed template length is unknown or unspecified, the template
     * length may estimated via the {@link #calculateTemplatePosition()} method.
     * 
     * @see #calculateTemplatePosition()
     */
    public void setTemplateLength(int templateLength) {
        this.templateLength = templateLength;
    }

    /**
     * Sets the template name.
     * 
     * @param templateName The templateName to set
     */
    public void setTemplateName(String templateName) {
        this.templateName.set(templateName);
    }

    /**
     * Sets the template name by copying the contents of <tt>templateName</tt>.
     * 
     * @param templateName The templateName to set
     */
    public void setTemplateName(Text templateName) {
        this.templateName.set(templateName);
    }

    /**
     * Sets the 1-based mapping position of the leftmost read associated with
     * this query template.
     */
    public void setTemplatePosition(int position) {
        templatePosition = position;
    }

    /**
     * Returns the number of reads that compose this template.
     * 
     * @return the number of reads that compose this template.
     */
    @Override
    public int size() {
        return size;
    }

    /**
     * Returns a {@link #size()}-index array containing all of the elements in
     * this list in proper sequence (from first to last element).
     * <p>
     * The returned array will be "safe" in that no references to it are
     * maintained by this list. The caller is thus free to modify the returned
     * array. The reads <i>within</i> the returned array, however, are
     * <i>not</i> copies and are not "safe" to modify.
     * <p>
     * This method acts as bridge between array-based and collection-based APIs.
     * 
     * @return a length {@link #size()} array containing all of the elements in
     *         this list in proper sequence.
     * @see Arrays#asList(Object[])
     */
    @Override
    public SequencingRead[] toArray() {
        SequencingRead[] array = new SequencingRead[size];

        System.arraycopy(readsArray, 0, array, 0, size);

        return array;
    }

    /**
     * Returns an array containing all of the elements in this list in proper
     * sequence (from first to last element); the runtime type of the returned
     * array is that of the specified array. If the list fits in the specified
     * array, it is returned therein. Otherwise, a new array is allocated with
     * the runtime type of the specified array and the size of this list.
     * <p>
     * If the list fits in the specified array with room to spare (i.e., the
     * array has more elements than the list), the element in the array
     * immediately following the end of the list is set to <tt>null</tt>.
     * <p>
     * Like the {@link #toArray()} method, this method acts as bridge between
     * array-based and collection-based APIs. Further, this method allows
     * precise control over the runtime type of the output array, and may, under
     * certain circumstances, be used to save allocation costs.
     * <p>
     * Suppose <tt>x</tt> is a list known to contain only sequencing reads. The
     * following code can be used to dump the list into a newly allocated array
     * of <tt>SequencingRead</tt>:
     * 
     * <pre>
     * SequencingRead[] y = x.toArray(new SequencingRead[0]);
     * </pre>
     * 
     * Note that <tt>toArray(new Object[0])</tt> is identical in function to
     * <tt>toArray()</tt>.
     * 
     * @param a the array into which the elements of this list are to be stored,
     *            if it is big enough; otherwise, a new array of the same
     *            runtime type is allocated for this purpose.
     * @return an array containing the elements of this list
     * @throws ArrayStoreException if the runtime type of the specified array is
     *             not a instance of {@link SequencingRead} (or a subtype).
     * @throws NullPointerException if the specified array is <code>null</code>.
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T[] toArray(T[] a) {
        if (a.length < size) {
            return (T[]) Arrays.copyOf(readsArray, size, a.getClass());
        }

        System.arraycopy(readsArray, 0, a, 0, size);

        if (a.length > size) {
            a[size] = null;
        }

        return a;
    }
    
    static class DataOutputWrapper implements DataOutput {
    	DataOutput out = null;
    	
    	public DataOutputWrapper(DataOutput out) {
    		this.out = out;
    	}

		public void write(int arg0) throws IOException {
			//System.out.println("Int: " + arg0);
			out.write(arg0);
		}

		public void write(byte[] arg0) throws IOException {
			//System.out.println("Write: " + arg0);
			out.write(arg0);
		}

		public void write(byte[] arg0, int arg1, int arg2) throws IOException {
			//System.out.println("Write: " + arg0 + ", " + arg1 + ", " + arg2);
			out.write(arg0, arg1, arg2);
		}

		public void writeBoolean(boolean arg0) throws IOException {
			//System.out.println("write: " + arg0);
			out.writeBoolean(arg0);
		}

		public void writeByte(int arg0) throws IOException {
			//System.out.println("Byt: " + arg0);
			out.writeByte(arg0);
		}

		public void writeBytes(String arg0) throws IOException {
			//System.out.println("Bts: " + arg0);
			out.writeBytes(arg0);
			
		}

		public void writeChar(int arg0) throws IOException {
			//System.out.println("Chr: " + arg0);
			out.writeChar(arg0);
		}

		public void writeChars(String arg0) throws IOException {
			//System.out.println("Str: " + arg0);
			out.writeChars(arg0);
		}

		public void writeDouble(double arg0) throws IOException {
			//System.out.println("Dbl: " + arg0);
			out.writeDouble(arg0);
		}

		public void writeFloat(float arg0) throws IOException {
			//System.out.println("Flt: " + arg0);
			out.writeFloat(arg0);
		}

		public void writeInt(int arg0) throws IOException {
			//System.out.println("Int: " + arg0);
			out.writeInt(arg0);
		}

		public void writeLong(long arg0) throws IOException {
			//System.out.println("Lng: " + arg0);
			out.writeLong(arg0);			
		}

		public void writeShort(int arg0) throws IOException {
			//System.out.println("Srt: " + arg0);
			out.writeShort(arg0);			
		}

		public void writeUTF(String arg0) throws IOException {
			//System.out.println("UTF: " + arg0);
			out.writeUTF(arg0);			
		}
    }

    int count = 5;
    
    /**
     * Serializes the fields of this object to <code>out</code>.
     * 
     * @param out <code>DataOuput</code> to serialize this object into.
     * @throws IOException if thrown by the underlying output stream.
     */
    public void write(DataOutput out) throws IOException {
//        if (count-- <= 0) {
//        	System.exit(0);
//        }
        //System.out.println("**********************");
    	out = new DataOutputWrapper(out);
    	
    	out.writeInt(size);

        for (int i = 0; i < size; i++) {
//        	System.out.println("readarr:"+i+":"+readsArray[i]);
        	readsArray[i].write(out, false);
        }

//        System.out.println("templatename:"+templateName);
        templateName.write(out);
//        System.out.println("templatelen:"+templateLength);
        out.writeInt(templateLength);
//        System.out.println("templatePosition:"+templatePosition);
        out.writeInt(templatePosition);
    }

    /**
     * Throws an exception if <code>index</code> is not in the range
     * <tt>0..size - 1</tt>
     * 
     * @throws IndexOutOfBoundsException if <tt>index &gt;= size</tt> or
     *             <tt>index &lt; 0</tt>.
     */
    private void checkBounds(int index) {
        if ((index < 0) || (index >= size)) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
        }
    }

    /**
     * If <code>elementData.length < minCapacity</code>, this method resizes
     * <code>elementData</code>. to ensure that it can hold at least
     * <code>minCapacity</code> elements.
     */
    private void ensureCapacity(int minCapacity) {
        modCount++;

        if (minCapacity > readsArray.length) {
            SequencingRead[] sr;
            int newCapacity = 1 + minCapacity + (minCapacity >> 2);

            // New array size is 1.25x minCapacity (plus 1).
            sr = new SequencingRead[newCapacity];

            System.arraycopy(readsArray, 0, sr, 0, size);

            readsArray = sr;

            // Also, is the static nulls pool large enough to accommodate?
            if (nullPool.length < newCapacity) {
                nullPool = new SequencingRead[newCapacity];
            }
        }
    }

    /**
     * Private remove method that skips bounds checking and does not return the
     * value removed.
     */
    private void fastRemove(int index) {
        modCount++;
        int numMoved = size - index - 1;

        if (numMoved > 0) {
            System.arraycopy(readsArray, index + 1, readsArray, index, numMoved);
        }

        readsArray[--size] = null;
    }

    private SequencingRead getPooledRead() {
        // If the read pool is empty, create a new instance.
        if ((readPoolPointer == 0) || (readPool[--readPoolPointer] == null)) {
            return new SequencingRead();
        } else {
            return readPool[readPoolPointer];
        }
    }

    /**
     * Removes the read at the specified index from the reads array and places
     * it into the read pool.
     * <p>
     * If the indicated position contains null, then no action is taken.
     * <p>
     * 
     * @param index The index of the read release into the read pool.
     */
    private void releaseRead(int index) {
        SequencingRead sr;
        
        if (null != (sr = readsArray[index])) {
            sr.setQueryTemplate(null);

            // If the pool hasn't been built yet, build it now and seed with the
            // indicated read.
            
            if (readPool.length == 0) {                
                resetReadPool(sr);
            } else if (readPoolPointer < readPool.length) {
                readPool[readPoolPointer++] = sr;
            }
        }

        readsArray[index] = null;
    }

    /**
     * Replaces the read at the specified index with a {@link SequencingRead}
     * instance from the read pool, and places the indicated read into the read
     * pool.
     * <p>
     * If the indicated position contains null, then nothing is copied into the
     * pool; an item from the pool is placed into the array and readPoolPointer
     * is incremented.
     * <p>
     * Note: this method is not synchronized (but refillPool() is).
     * <p>
     * 
     * @param index The index of the read to swap with an read from the read
     *            pool.
     */
    @SuppressWarnings("unused")
    private void releaseReadAndSwap(int index) {
        SequencingRead swapIn = getPooledRead();

        releaseRead(index);

        readsArray[index] = swapIn;
    }

    /**
     * If the size if the read pool is less than the indicated size then this
     * method resizes it to the number of unused readsArray positions and resets
     * the read pool pointer.
     * 
     * @param primerWith A list of reads to be placed into the first positions
     *            of the pool (instead of creating new instances).
     */
    private synchronized void resetReadPool(int minSize, SequencingRead... primeWith) {
        int numInstances = 1;

        if (minSize < primeWith.length) {
            minSize = primeWith.length;
        }

        if (minSize < numInstances) {
            minSize = numInstances;
        }

        if (readPool.length > minSize) {
            return;
        }

        SequencingRead[] newPool = new SequencingRead[minSize];

        if (numInstances < primeWith.length) {
            numInstances = primeWith.length;
        }

        int i = 0;
        for (int r = 0, p = 0; i < numInstances; i++) {
            // Use non-null values from the primeWith array first.
            if ((p < primeWith.length) && (primeWith[p] != null)) {
                newPool[i] = primeWith[p++];
            } else if ((r < readPool.length) && (readPool[r] != null)) {
                newPool[i] = readPool[r++];
            } else {
                newPool[i] = new SequencingRead();
            }
        }

        readPoolPointer = i;
        readPool = newPool;
    }

    /**
     * If the size if the read pool is less than the number of unused positions
     * in readsArray (if readsArray.length - size), this method resizes it to
     * the number of unused readsArray positions and resets the read pool
     * pointer.
     * 
     * @param primerWith A list of reads to be placed into the first positions
     *            of the pool (instead of creating new instances).
     */
    private synchronized void resetReadPool(SequencingRead... primeWith) {
        resetReadPool(readsArray.length - size, primeWith);
    }
}
