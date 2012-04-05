/*
 * Copyright 2011 Matthew A. Titmus
 * 
 * This file is part of Jnomics.
 * 
 * Jnomics is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 * 
 * Jnomics is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with
 * Jnomics. If not, see <http://www.gnu.org/licenses/>.
 */

package edu.cshl.schatz.jnomics.mapreduce;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.cshl.schatz.jnomics.ob.writable.QueryTemplate;
import edu.cshl.schatz.jnomics.ob.writable.SequencingRead;

/**
 * @author Matthew A. Titmus
 */
public abstract class JnomicsRecordWriter<K, V> extends RecordWriter<K, V> {
    /**
     * A UTF8-encoded newline character.
     */
    public static final byte NEWLINE;

    /**
     * A UTF8-encoded tab character.
     */
    public static final byte TAB;

    /**
     * The name of the UTF-8 encoding character set as understood by Java.
     */
    protected static final String UTF8 = "UTF-8";

    protected final Configuration config;

    protected final DataOutputStream out;

    private Text t = new Text();

    static {
        try {
            NEWLINE = "\n".getBytes(UTF8)[0];
            TAB = "\t".getBytes(UTF8)[0];
        } catch (UnsupportedEncodingException uee) {
            throw new IllegalArgumentException("Can't find " + UTF8 + " encoding.");
        }
    }

    /**
     * @param out The {@link DataOutputStream} to write to.
     * @param config The {@link Configuration} of the user class.
     */
    public JnomicsRecordWriter(DataOutputStream out, Configuration config) {
        this.out = out;
        this.config = config;
    }

    /**
     * @param path The {@link Path} to write to.
     * @param config The {@link Configuration} of the user class.
     * @throws IOException
     */
    public JnomicsRecordWriter(Path path, Configuration config) throws IOException {
        out = FileSystem.getLocal(config).create(path);
        this.config = config;
    }

    /*
     * @see
     * org.apache.hadoop.mapreduce.RecordWriter#close(org.apache.hadoop.mapreduce
     * .TaskAttemptContext)
     */
    @Override
    public synchronized void close(TaskAttemptContext context) throws IOException {
        out.close();
    }

    /**
     * Writes the key and value to the {@link DataOutputStream}. The default
     * implementation ignores <code>key</code> and passes <code>value</code> to
     * either (@link {@link #writeQueryTemplate(QueryTemplate)} or
     * {@link #writeSequencingRead(SequencingRead)}, depending on its class.
     * 
     * @throws IOException If thrown by the underlying {@link OutputStream}
     *             write operation.
     * @see org.apache.hadoop.mapreduce.RecordWriter#write(java.lang.Object,
     *      java.lang.Object)
     */
    @Override
    public synchronized void write(K key, V value) throws IOException {
        if ((value == null) || (value instanceof NullWritable)) {
            return;
        }

        if (value instanceof QueryTemplate) {
            writeQueryTemplate((QueryTemplate) value);
        } else if (value instanceof SequencingRead) {
            writeSequencingRead((SequencingRead) value);
        } else {
            throw new UnsupportedOperationException("Unsupported value class: "
                    + value.getClass().toString());
        }
    }

    /**
     * A helper method that simply writes a UTF8-encoded newline character to
     * the output stream.
     * 
     * @throws IOException If thrown by the underlying {@link OutputStream}
     *             write operation.
     */
    protected final void writeNewline() throws IOException {
        out.writeByte(NEWLINE);
    }

    /**
     * Writes an integer as text value.
     * 
     * @param i The integer to print.
     * @throws IOException If thrown by the underlying {@link OutputStream}
     *             write operation.
     */
    protected void writeObject(int i) throws IOException {
        t.set(Integer.toString(i));
        out.write(t.getBytes(), 0, t.getLength());
    }

    /**
     * Write the object to the byte stream, handling Text as a special case.
     * 
     * @param o the object to print
     * @throws IOException If thrown by the underlying {@link OutputStream}
     *             write operation.
     */
    protected void writeObject(Object o) throws IOException {
        if (o instanceof Text) {
            Text to = (Text) o;
            out.write(to.getBytes(), 0, to.getLength());
        } else {
            out.write(o.toString().getBytes(UTF8));
        }
    }

    /**
     * Writes a query template to the {@link DataOutputStream}. The default
     * implementation calls {@link #writeSequencingRead(SequencingRead)} for
     * each read in the query template.
     * 
     * @param template The query template to output to the stream.
     * @throws IOException If thrown by the underlying {@link OutputStream}
     *             write operation.
     */
    protected void writeQueryTemplate(QueryTemplate template) throws IOException {
        for (SequencingRead read : template) {
            writeSequencingRead(read);
        }
    }

    /**
     * Writes a sequencing read to the {@link DataOutputStream}. The default
     * implementation does nothing.
     * 
     * @param read The sequencing read to output to the stream.
     * @throws IOException If thrown by the underlying {@link OutputStream}
     *             write operation.
     */
    protected void writeSequencingRead(SequencingRead read) throws IOException {}

    /**
     * A helper method that simply writes a UTF8-encoded tab character to the
     * output stream.
     * 
     * @throws IOException If thrown by the underlying {@link OutputStream}
     *             write operation.
     */
    protected final void writeTab() throws IOException {
        out.writeByte(TAB);
    }
}
