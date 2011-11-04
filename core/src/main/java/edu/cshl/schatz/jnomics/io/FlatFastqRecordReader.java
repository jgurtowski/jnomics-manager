/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: Wed Sep 21 18:29:28 EDT 2011 (revision 835)
 */

package edu.cshl.schatz.jnomics.io;

import static edu.cshl.schatz.jnomics.ob.writable.SequencingRead.FLAG_FIRST_SEGMENT;
import static edu.cshl.schatz.jnomics.ob.writable.SequencingRead.FLAG_LAST_SEGMENT;
import static edu.cshl.schatz.jnomics.ob.writable.SequencingRead.FLAG_MULTIPLE_FRAGMENTS;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import edu.cshl.schatz.jnomics.ob.IUPACCodeException;
import edu.cshl.schatz.jnomics.ob.writable.QueryTemplate;
import edu.cshl.schatz.jnomics.ob.writable.SequencingRead;

/**
 * This record reader reads short-read sequence files, and breaks the data into
 * key/value pairs for input to the Mapper.
 * 
 * @author Matthew A. Titmus
 */
public class FlatFastqRecordReader extends JnomicsFileRecordReader {
    private static final Log LOG = LogFactory.getLog(FlatFastqRecordReader.class);

    private CompressionCodecFactory compressionCodecs = null;

    private int maxLineLength = Integer.MAX_VALUE;

    /**
     * @param split
     * @param conf
     * @throws IOException
     */
    @Override
    public void initialize(FileSplit split, Configuration conf) throws IOException {
        compressionCodecs = new CompressionCodecFactory(conf);

        final Path file = split.getPath();
        final CompressionCodec codec = compressionCodecs.getCodec(file);

        maxLineLength = conf.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);

        splitStart = split.getStart();
        splitEnd = splitStart + split.getLength();

        FileSystem fs = file.getFileSystem(conf);
        FSDataInputStream fileIn = fs.open(split.getPath());
        boolean skipFirstLine = false;

        if (codec != null) {
            in = new FlatFastqLineReader(codec.createInputStream(fileIn), conf);
            splitEnd = Long.MAX_VALUE;
        } else {
            if (splitStart != 0) {
                skipFirstLine = true;
                --splitStart;
                fileIn.seek(splitStart);
            }
            in = new FlatFastqLineReader(fileIn, conf);
        }

        if (skipFirstLine) { // skip first line and re-establish "start".
            splitStart += in.readLine(
                new Text(), 0, (int) Math.min(Integer.MAX_VALUE, splitEnd - splitStart));
        }

        pos = splitStart;
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        if (key == null) {
            key = new Text();
        }

        if (value == null) {
            value = new QueryTemplate();
        }

        int newSize = 0;
        while (pos < splitEnd) {
            newSize = ((FlatFastqLineReader) in).readRecord(
                value, maxLineLength,
                Math.max((int) Math.min(Integer.MAX_VALUE, splitEnd - pos), maxLineLength));

            if (newSize == 0) {
                break;
            }

            pos += newSize;

            if (newSize < maxLineLength) {
                break;
            }

            // line too long. try again
            LOG.info("Skipped line of size " + newSize + " at pos " + (pos - newSize));
        }

        if (newSize == 0) {
            key = null;
            value = null;

            return false;
        } else {
            key.set(value.getTemplateName());

            return true;
        }
    }

    /**
     * This reads a short read DNA sequence from a text file, one line at a
     * time.
     */
    public static class FlatFastqLineReader extends RecordLineReader {
        /**
         * Create a reader that reads from the given stream using the default
         * buffer-size (64k).
         * 
         * @param in The input stream
         * @throws IOException
         */
        public FlatFastqLineReader(InputStream in) {
            super(in);
        }

        /**
         * Create a reader that reads from the given stream using the
         * <code>io.file.buffer.size</code> specified in the given
         * {@link Configuration}.
         * 
         * @param in The input stream
         * @param conf The configuration
         */
        public FlatFastqLineReader(InputStream in, Configuration conf) {
            super(in, conf);
        }

        /**
         * Create a line reader that reads from the given stream using the given
         * buffer-size.
         * 
         * @param in The input stream
         * @param bufferSize Size of the read buffer
         * @throws IOException
         */
        public FlatFastqLineReader(InputStream in, int bufferSize) {
            super(in, bufferSize);
        }

        /**
         * Reads one or more lines from the InputStream into the given
         * {@link QueryTemplate}. A line can be terminated by one of the
         * following: '\n' (LF) , '\r' (CR), or '\r\n' (CR+LF). EOF also
         * terminates an otherwise unterminated line.
         * 
         * @param queryTemplate the object to store the next one or more
         *            records.
         * @return the number of bytes read including the (longest) newline
         *         found.
         * @throws IOException if thrown by the underlying stream.
         * @throws IUPACCodeException
         */
        public boolean parseText(QueryTemplate queryTemplate, Text text) throws IOException {
            queryTemplate.clear();

            textCutter.set(text, 0, text.getLength());
            textCutter.getCut(queryTemplate.getTemplateName(), 0);

            SequencingRead read;
            Text column = null, readName;

            if (textCutter.getCutCount() >= 3) {
                read = queryTemplate.getEmptyRead();
                readName = read.getReadName();
                
                readName.set(queryTemplate.getTemplateName());
                readName.append(SLASH_FIRST, 0, 2);

                column = textCutter.getCut(1);
                read.setSequence(column.getBytes(), 0, column.getLength(), true);

                textCutter.getCut(read.getPhredString(), 2);
                read.setFlags(FLAG_FIRST_SEGMENT + FLAG_MULTIPLE_FRAGMENTS);
                queryTemplate.add(read);
            }

            if (textCutter.getCutCount() >= 5) {
                read = queryTemplate.getEmptyRead();
                readName = read.getReadName();
                
                readName.set(queryTemplate.getTemplateName());
                readName.append(SLASH_LAST, 0, 2);

                column = textCutter.getCut(3);
                read.setSequence(column.getBytes(), 0, column.getLength(), true);

                textCutter.getCut(read.getPhredString(), 4);
                read.setFlags(FLAG_LAST_SEGMENT + FLAG_MULTIPLE_FRAGMENTS);
                queryTemplate.add(read);
            }

            for (int i = 5; i < textCutter.getCutCount(); i++) {
                read = queryTemplate.getEmptyRead();
                readName = read.getReadName();
                
                readName.set(queryTemplate.getTemplateName());
                // Don't give these a trailing slash value

                column = textCutter.getCut(i);
                read.setSequence(column.getBytes(), 0, column.getLength(), true);

                textCutter.getCut(read.getPhredString(), i + 1);
                read.setFlags(FLAG_FIRST_SEGMENT + FLAG_LAST_SEGMENT + FLAG_MULTIPLE_FRAGMENTS);
                queryTemplate.add(read);
            }

            return false;
        }

        /**
         * Reads one or more lines from the InputStream into the given
         * {@link QueryTemplate}. A line can be terminated by one of the
         * following: '\n' (LF) , '\r' (CR), or '\r\n' (CR+LF). EOF also
         * terminates an otherwise unterminated line.
         * 
         * @param queryTemplate the object to store the next one or more
         *            records.
         * @return the number of bytes read including the (longest) newline
         *         found.
         * @throws IOException if thrown by the underlying stream.
         * @throws IUPACCodeException
         */
        @Override
        public int readRecord(QueryTemplate queryTemplate) throws IOException {
            return readLine(line, Integer.MAX_VALUE, Integer.MAX_VALUE);
        }

        /**
         * Reads one or more lines from the InputStream into the given
         * {@link QueryTemplate}. A line can be terminated by one of the
         * following: '\n' (LF) , '\r' (CR), or '\r\n' (CR+LF). EOF also
         * terminates an otherwise unterminated line.
         * 
         * @param queryTemplate the object to store the next one or more
         *            records.
         * @return the number of bytes read including the (longest) newline
         *         found.
         * @throws IOException if thrown by the underlying stream.
         * @throws IUPACCodeException
         */
        @Override
        public int readRecord(QueryTemplate queryTemplate, int maxLineLength, int maxBytesToConsume)
                throws IOException {

            int bytesConsumed = readLine(line, maxLineLength, maxBytesToConsume);

            parseText(queryTemplate, line);

            return bytesConsumed;
        }
    }
}
