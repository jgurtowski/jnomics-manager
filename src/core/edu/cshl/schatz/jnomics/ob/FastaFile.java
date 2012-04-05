/*
 * Copyright (C) 2011 Matthew A. Titmus
 */

package edu.cshl.schatz.jnomics.ob;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import edu.cshl.schatz.jnomics.io.FileFormatException;

/**
 * <p>
 * Represents an NCBI-compliant FASTA file (specification available at
 * http://www.ncbi.nlm.nih.gov/blast/fasta.shtml. Acceptable characters are as
 * follows:
 * 
 * <pre>
 * A   Adenosine
 * C   Cytosine
 * G   Guanine
 * T   Thymidine
 * U   Uracil
 * R   G A (puRine)
 * Y   T C (pYrimidine)
 * K   G T (Ketone)
 * M   A C (aMino group)
 * S   G C (Strong interaction)
 * W   A T (Weak interaction)
 * B   G T C (not A) (B comes after A)
 * D   G A T (not C) (D comes after C)
 * H   A C T (not G) (H comes after G)
 * V   G C A (not T, not U) (V comes after U)
 * N   A G C T (aNy)
 * X   masked
 * -   gap of indeterminate length
 * </pre>
 * 
 * </p>
 * <p>
 * This class implements the {@link List} interface; all inherited methods are
 * passed to an internal unmodifiable List instance.
 * </p>
 * TODO Re-implement using the native IO libraries.
 * 
 * @author Matthew Titmus
 */
public class FastaFile implements List<FastaFileEntry> {
    /**
     * An unmodifiable list.
     */
    private List<FastaFileEntry> entryList;

    /**
     * Instantiates an instance from an existing FASTA file in the local file
     * system.
     * 
     * @param fastaFile A {@link File} object representing a FASTA file.
     * @throws FileFormatException If the file doesn't contain a valid FASTA
     *             file.
     * @throws IOException in the event of a failed or interrupted I/O
     *             operation.
     */
    public FastaFile(File fastaFile) throws IOException {
        this(new FileInputStream(fastaFile));
    }

    /**
     * Used to instantiate a {@link FastaFile} object from a stream.
     * <p>
     * TODO This currently loads the ENTIRE sequence into memory before creating
     * a new FASTA entry. Clearly, this is suboptimal. Fix it.
     * </p>
     * 
     * @throws FileFormatException If the file doesn't contain a valid FASTA
     *             file.
     * @throws IOException in the event of a failed or interrupted I/O
     *             operation.
     */
    public FastaFile(InputStream istream) throws IOException {
        BufferedReader data = new BufferedReader(new InputStreamReader(istream));
        List<FastaFileEntry> list = new ArrayList<FastaFileEntry>();
        int lineNumber = 0;

        // These are used to hold the entry values before we create the Entry
        // instance.
        StringBuffer entrySequence = new StringBuffer(1024);
        String entryHeader = "";
        int entryIdxInFile = 0;

        try {
            String line;
            while (null != (line = data.readLine())) {
                lineNumber++;

                if (line.length() == 0) {
                    // Ignore blank lines.

                    continue;
                } else if ((line.charAt(0) == '>') || (line.charAt(0) == ';')) {
                    // FASTA headers begin with a < or (rarely) ;.

                    if (entryHeader.length() == 0) {
                        entryHeader = line.substring(1).trim();
                    } else {
                        if (entrySequence.length() == 0) {
                            // Probably a comment line. This should be rare.
                            continue;
                        } else {
                            try {
                                list.add(new FastaFileEntry(
                                    entryHeader, entrySequence, ++entryIdxInFile));
                            } catch (IUPACCodeException e) {
                                throw new FileFormatException(e);
                            }

                            entryHeader = "";
                            entrySequence.setLength(0);
                        }
                    }
                } else if (lineNumber == 1) {
                    // If there's no header at the top of the file, we throw an
                    // exception.
                    throw new FileFormatException("No header found");
                } else {
                    entrySequence.append(line);
                }
            }
        } catch (RuntimeException e) {
            System.err.println("Exception while handling line " + lineNumber);

            throw e;
        }

        try {
            list.add(new FastaFileEntry(entryHeader, entrySequence, ++entryIdxInFile));
        } catch (IUPACCodeException e) {
            throw new FileFormatException(e);
        }

        entryList = Collections.unmodifiableList(list);
    }

    /**
     * This method attempts to glean a useful identifier from the FASTA entry's
     * header. The FASTA format is notoriously non-standard, but generally an
     * reasonable id can be pulled from most institutions' FASTA headers by
     * simply grabbing the first word. Good enough for now.
     */
    public static String guessIdentifierFromHeader(String header) {
        int index = header.indexOf(' ');

        return index == -1 ? header : header.substring(0, index);
    }

    @SuppressWarnings("all")
    public static void main(String[] args) {
        try {
            Tests.testLoadFASTA();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*
     * @see java.util.List#add(java.lang.Object)
     */
    public boolean add(FastaFileEntry e) {
        throw new UnsupportedOperationException();
    }

    /*
     * @see java.util.List#add(int, java.lang.Object)
     */
    public void add(int index, FastaFileEntry element) {
        throw new UnsupportedOperationException();
    }

    /*
     * @see java.util.List#addAll(java.util.Collection)
     */
    public boolean addAll(Collection<? extends FastaFileEntry> c) {
        throw new UnsupportedOperationException();
    }

    /*
     * @see java.util.List#addAll(int, java.util.Collection)
     */
    public boolean addAll(int index, Collection<? extends FastaFileEntry> c) {
        throw new UnsupportedOperationException();
    }

    /*
     * @see java.util.List#clear()
     */
    public void clear() {
        throw new UnsupportedOperationException();
    }

    /*
     * @see java.util.List#contains(java.lang.Object)
     */
    public boolean contains(Object o) {
        return entryList.contains(o);
    }

    /*
     * @see java.util.List#containsAll(java.util.Collection)
     */
    public boolean containsAll(Collection<?> c) {
        return entryList.containsAll(c);
    }

    /*
     * @see java.util.List#get(int)
     */
    public FastaFileEntry get(int index) {
        return entryList.get(index);
    }

    /*
     * @see java.util.List#indexOf(java.lang.Object)
     */
    public int indexOf(Object o) {
        return entryList.indexOf(o);
    }

    /*
     * @see java.util.List#isEmpty()
     */
    public boolean isEmpty() {
        return entryList.isEmpty();
    }

    /*
     * @see java.util.List#iterator()
     */
    public Iterator<FastaFileEntry> iterator() {
        return entryList.iterator();
    }

    /*
     * @see java.util.List#lastIndexOf(java.lang.Object)
     */
    public int lastIndexOf(Object o) {
        return entryList.lastIndexOf(o);
    }

    /*
     * @see java.util.List#listIterator()
     */
    public ListIterator<FastaFileEntry> listIterator() {
        return listIterator();
    }

    /*
     * @see java.util.List#listIterator(int)
     */
    public ListIterator<FastaFileEntry> listIterator(int index) {
        return entryList.listIterator(index);
    }

    /**
     * Writes FASTA-formatted text of this instance and all entries within an
     * {@link OutputStream}.
     * 
     * @param out An instance of {@link OutputStream}.
     * @throws IOException
     */
    public void outputTo(OutputStream out) throws IOException {
        for (FastaFileEntry entry : entryList) {
            entry.outputTo(out);
        }
    }

    /*
     * @see java.util.List#remove(int)
     */
    public FastaFileEntry remove(int index) {
        return remove(index);
    }

    /*
     * @see java.util.List#remove(java.lang.Object)
     */
    public boolean remove(Object o) {
        return remove(o);
    }

    /*
     * @see java.util.List#removeAll(java.util.Collection)
     */
    public boolean removeAll(Collection<?> c) {
        return removeAll(c);
    }

    /*
     * @see java.util.List#retainAll(java.util.Collection)
     */
    public boolean retainAll(Collection<?> c) {
        return retainAll(c);
    }

    /*
     * @see java.util.List#set(int, java.lang.Object)
     */
    public FastaFileEntry set(int index, FastaFileEntry element) {
        return entryList.set(index, element);
    }

    /**
     * Returns the number of entries contained within this FASTA file. In most
     * cases this will be 1, but the FASTA specification permits an unlimited
     * number of entries.
     */
    public int size() {
        return entryList.size();
    }

    /*
     * @see java.util.List#subList(int, int)
     */
    public List<FastaFileEntry> subList(int fromIndex, int toIndex) {
        return entryList.subList(fromIndex, toIndex);
    }

    /*
     * @see java.util.List#toArray()
     */
    public Object[] toArray() {
        return entryList.toArray();
    }

    /*
     * @see java.util.List#toArray(T[])
     */
    public <T> T[] toArray(T[] a) {
        return entryList.toArray(a);
    }

    @SuppressWarnings("all")
    private static class Tests {
        private static void testLoadFASTA() throws IOException {
            File f = new File("/home/mtitmus/NG_000007_70545_72150.fa");
            FastaFile fastaFile = new FastaFile(f);
            FastaFileEntry ffe;

            ffe = fastaFile.get(0);
            System.out.printf("Index in file: %1$s%n", ffe.getIndexInFile());
            System.out.printf("Header: %1$s%n", ffe.getHeader());
            System.out.printf("Identifier: %1$s%n", ffe.getIdentifier());
            System.out.printf("SeqLen: %1$s%n", ffe.length());
            System.out.printf("Entropy: %1$fd%n", ffe.getEntropy());
            System.out.printf("EntropyMax: %1$f%n", ffe.getEntropyMax());
            System.out.println();

            // Iterator<FastaFileEntry> iter = fastaFile.iterator();
            // while (iter.hasNext()) {
            // ffe = iter.next();
            //
            // System.out.printf(
            // " %1$3d: %2$s (%3$dbp)%n",
            // ffe.getIndexInFile(),
            // ffe.getIdentifier(),
            // ffe.length()
            // );
            // }
        }
    }
}
