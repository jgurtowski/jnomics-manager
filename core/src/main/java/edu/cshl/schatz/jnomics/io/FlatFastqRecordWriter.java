/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.io;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import edu.cshl.schatz.jnomics.mapreduce.JnomicsRecordWriter;
import edu.cshl.schatz.jnomics.ob.writable.QueryTemplate;
import edu.cshl.schatz.jnomics.ob.writable.SequencingRead;

/**
 * @author Matthew A. Titmus
 */
public class FlatFastqRecordWriter<K, V> extends JnomicsRecordWriter<K, V> {
    private static final Log LOG = LogFactory.getLog(FlatFastqRecordWriter.class.getSimpleName());

    private SequencingRead firstRead, lastRead;

    /**
     * @param out The {@link DataOutputStream} to write to.
     * @param config The {@link Configuration} of the user class.
     */
    public FlatFastqRecordWriter(DataOutputStream out, Configuration config) {
        super(out, config);
    }

    /*
     * @see
     * edu.cshl.schatz.jnomics.io.JnomicsRecordWriter#writeQueryTemplate(edu
     * .cshl.schatz.jnomics.util.QueryTemplate)
     */
    @Override
    protected void writeQueryTemplate(QueryTemplate template) throws IOException {
        firstRead = lastRead = null;

        for (SequencingRead read : template) {
            if (read.hasAllFlags(SequencingRead.FLAG_REVERSE_COMPLEMENTED)) {
                read.reverseComplement();
            }

            if (read.hasAllFlags(SequencingRead.FLAG_FIRST_SEGMENT)) {
                firstRead = read;
            } else if (read.hasAllFlags(SequencingRead.FLAG_LAST_SEGMENT)) {
                lastRead = read;
            } else {
                LOG.warn("Unpaired read: " + read.getReadName());
            }
        }

        // Only write these if we have both ends of the read. If not, just
        // note a singleton.
        if ((firstRead != null) && (lastRead != null)) {
            writeObject(template.getTemplateNameString());
            writeTab();

            out.write(firstRead.getRawBytes(), 0, firstRead.length());
            writeTab();

            writeObject(firstRead.getPhredString());
            writeTab();

            out.write(lastRead.getRawBytes(), 0, lastRead.length());
            writeTab();

            writeObject(lastRead.getPhredString());
            writeNewline();
        }
    }
}
