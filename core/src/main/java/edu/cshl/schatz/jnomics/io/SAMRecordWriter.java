/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.io;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import edu.cshl.schatz.jnomics.mapreduce.JnomicsRecordWriter;
import edu.cshl.schatz.jnomics.ob.writable.SequencingRead;

/**
 * @author Matthew A. Titmus
 */
public class SAMRecordWriter<K, V> extends JnomicsRecordWriter<K, V> {
    private static final byte ASTERISK;

    private static final byte COLON;
    static {
        try {
            COLON = ":".getBytes(UTF8)[0];
            ASTERISK = "*".getBytes(UTF8)[0];
        } catch (UnsupportedEncodingException uee) {
            throw new IllegalArgumentException("can't find " + UTF8 + " encoding");
        }
    }

    /**
     * @param out The {@link DataOutputStream} to write to.
     * @param config The {@link Configuration} of the user class.
     */
    public SAMRecordWriter(DataOutputStream out, Configuration config) {
        super(out, config);
    }

    public SAMRecordWriter(Path path, Configuration config) throws IOException {
        super(path, config);
    }

    /*
     * @see
     * edu.cshl.schatz.jnomics.io.JnomicsRecordWriter#writeObject(java.lang.
     * Object)
     */
    @Override
    protected void writeObject(Object o) throws IOException {
        // In SAM format, blank fields are written as an asterisk (*).
        if ((o instanceof Text) && (((Text) o).getLength() == 0)) {
            out.writeByte(ASTERISK);
        } else {
            super.writeObject(o);
        }
    }

    /*
     * @see
     * edu.cshl.schatz.jnomics.io.JnomicsRecordWriter#writeSequencingRead(edu
     * .cshl.schatz.jnomics.util.SequencingRead)
     */
    @Override
    protected void writeSequencingRead(SequencingRead read) throws IOException {        
        writeObject(read.getQueryTemplate().getTemplateName());
        writeTab();

        writeObject(read.getFlags());
        writeTab();

        writeObject(read.getReferenceNameText());
        writeTab();

        writeObject(read.getMappingPosition());
        writeTab();

        writeObject(read.getMappingQuality());
        writeTab();

        writeObject(read.getCigar());
        writeTab();

        writeObject(read.getNextReferenceName());
        writeTab();

        writeObject(read.getNextPosition());
        writeTab();

        if (read.isReverseComplemented() && read.getQueryTemplate().getTemplateLength() > 0) {
            out.writeByte('-');
        }
        writeObject(read.getQueryTemplate().getTemplateLength());
        writeTab();

        out.write(read.getRawBytes(), 0, read.length());
        writeTab();

        writeObject(read.getPhredString());

        for (Entry<Writable, Writable> e : read.getProperties().entrySet()) {
            writeTab();
            writeObject(e.getKey());
            out.writeByte(COLON);
            writeObject(e.getValue());
        }

        writeNewline();
    }
}
