/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.io;

import static edu.cshl.schatz.jnomics.ob.writable.SequencingRead.FLAG_FIRST_SEGMENT;
import static edu.cshl.schatz.jnomics.ob.writable.SequencingRead.FLAG_LAST_SEGMENT;
import static edu.cshl.schatz.jnomics.ob.writable.SequencingRead.FLAG_REVERSE_COMPLEMENTED;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import edu.cshl.schatz.jnomics.mapreduce.JnomicsRecordWriter;
import edu.cshl.schatz.jnomics.ob.writable.SequencingRead;

/**
 * @author Matthew A. Titmus
 */
public class FastqRecordWriter<K, V> extends JnomicsRecordWriter<K, V> {
    public static final int MEMBER_ANY = 255;

    public static final int MEMBER_FIRST = FLAG_FIRST_SEGMENT;

    public static final int MEMBER_FIRST_AND_LAST = FLAG_FIRST_SEGMENT + FLAG_LAST_SEGMENT;

    public static final int MEMBER_LAST = FLAG_LAST_SEGMENT;

    private static final byte[] AMPERSAND;

    private static final byte[] LINE1_FIRST;

    private static final byte[] LINE1_LAST;

    private static final byte[] LINE3_BYTES;

    private int matchFlags = 0;

    private Text text = new Text();

    static {
        try {
            AMPERSAND = "@".getBytes(UTF8);
            LINE3_BYTES = "+\n".getBytes(UTF8);
            LINE1_FIRST = "/1".getBytes(UTF8);
            LINE1_LAST = "/2".getBytes(UTF8);
        } catch (UnsupportedEncodingException uee) {
            throw new IllegalArgumentException("Can't find " + UTF8 + " encoding.");
        }
    }

    /**
     * @param out The {@link DataOutputStream} to write to.
     * @param config The {@link Configuration} of the user class.
     * @param member A MEMBER_* constant indicating whether member of a read
     *            pair this should write: <code>MEMBER_FIRST</code>=first
     *            ("/1"), <code>MEMBER_LAST</code>=last ("/2"), or
     *            <code>MEMBER_FIRST_AND_LAST</code>=either first or last("/1"
     *            or "/2").
     */
    public FastqRecordWriter(DataOutputStream out, Configuration config, int member) {
        super(out, config);
        this.matchFlags = member;
    }

    /**
     * @param path
     * @param config
     * @param member
     * @throws IOException
     */
    public FastqRecordWriter(Path path, Configuration config, int member) throws IOException {
        super(path, config);
        this.matchFlags = member;
    }

    /*
     * @see
     * edu.cshl.schatz.jnomics.io.JnomicsRecordWriter#writeSequencingRead(edu
     * .cshl.schatz.jnomics.util.SequencingRead)
     */
    @Override
    protected void writeSequencingRead(SequencingRead read) throws IOException {
        byte[] bytes = null;
        int length;

        if (read.hasAnyFlags(matchFlags)) {
            if (read.hasAnyFlags(FLAG_REVERSE_COMPLEMENTED)) {
                read.reverseComplement();
            }

            { // Line 1: "@SEQ_ID/1"
                if (read.getQueryTemplate() != null) {
                    bytes = read.getQueryTemplate().getTemplateName().getBytes();
                    length = read.getQueryTemplate().getTemplateName().getLength();
                } else {
                    bytes = read.getReadName().getBytes();
                    length = read.getReadName().getLength();

                    if ((bytes[length - 2] == LINE1_FIRST[0])
                            && ((bytes[length - 1] == LINE1_FIRST[1]) && (bytes[length - 1] == LINE1_LAST[1]))) {

                        length -= 2;
                    }
                }

                text.set(AMPERSAND);
                text.append(bytes, 0, length);

                if (read.hasFlag(FLAG_FIRST_SEGMENT)) {
                    text.append(LINE1_FIRST, 0, LINE1_FIRST.length);
                } else if (read.hasFlag(FLAG_LAST_SEGMENT)) {
                    text.append(LINE1_LAST, 0, LINE1_LAST.length);
                }

                writeObject(text);
                writeNewline();
            }

            { // Line 2: "[ACTG]+"
                out.write(read.getRawBytes(), 0, read.length());
                writeNewline();
            }

            { // Line 3: "+"
                out.write(LINE3_BYTES);
            }

            { // Line 4: Phred string
                bytes = read.getPhredString().getBytes();
                length = read.getPhredString().getLength();

                out.write(bytes, 0, length);
                writeNewline();
            }

            out.flush();
        }
    }
}
