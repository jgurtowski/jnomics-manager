/*
 * Copyright (C) 2011 Matthew A. Titmus
 */

package edu.cshl.schatz.jnomics.ob;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

import edu.cshl.schatz.jnomics.io.DNASequenceReader;

public class FastaFileEntry extends DNASequence {
    private String header;

    private int indexInFile;

    /**
     * @param header
     * @param sequence
     */
    public FastaFileEntry(String header, CharSequence sequence) {
        super(FastaFile.guessIdentifierFromHeader(header), sequence, 1, Orientation.PLUS);

        this.header = header;
    }

    /**
     * @param header
     * @param identifier
     * @param sequence
     */
    public FastaFileEntry(String header, String identifier, CharSequence sequence) {

        super(identifier, sequence, 1, Orientation.PLUS);

        this.header = header;
    }

    /**
     * @param header
     * @param sequence
     * @param indexInFile
     * @throws IUPACCodeException if an invalid nucleotide code is used (see
     *             http://www.bioinformatics.org/sms/iupac.html).
     */
    FastaFileEntry(String header, CharSequence sequence, int indexInFile) {
        super(FastaFile.guessIdentifierFromHeader(header), sequence, 1, Orientation.PLUS);

        this.header = header;
        this.indexInFile = indexInFile;
    }

    /**
     * @return The header
     */
    public String getHeader() {
        return header;
    }

    /**
     * @return The indexInFile
     */
    public int getIndexInFile() {
        return indexInFile;
    }

    /**
     * Outputs this entry in FASTA format to an {@link OutputStream}.
     * 
     * @param out An instance of {@link OutputStream}.
     * @throws IOException
     */
    public void outputTo(OutputStream out) throws IOException {
        PrintStream ostream;
        DNASequenceReader reader = getSequenceReader();

        if (out instanceof PrintStream) {
            ostream = (PrintStream) out;
        } else {
            ostream = new PrintStream(out);
        }

        ostream.println('>' + getHeader());

        String line;

        while (null != (line = reader.readLine())) {
            ostream.println(line);
        }

        ostream.flush();
        reader.close();
    }

    /**
     * @param header The header to set
     */
    void setHeader(String header) {
        this.header = header;
    }

    /**
     * @param indexInFile The indexInFile to set
     */
    void setIndexInFile(int indexInFile) {
        this.indexInFile = indexInFile;
    }
}
