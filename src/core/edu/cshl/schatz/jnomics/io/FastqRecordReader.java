/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.io;

import java.io.IOException;
import java.io.InputStream;
import java.util.regex.Pattern;

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
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.util.LineReader;

import edu.cshl.schatz.jnomics.ob.header.HeaderData;
import edu.cshl.schatz.jnomics.ob.writable.QueryTemplate;
import edu.cshl.schatz.jnomics.ob.writable.SequencingRead;

/**
 * This record reader reads short-read sequence files, and breaks the data into
 * key/value pairs for input to the Mapper.
 * 
 * @author Matthew A. Titmus
 */
public class FastqRecordReader extends JnomicsFileRecordReader {
    private static final Pattern COMMENT_PATTERN = Pattern.compile("^\\+.*$");

    private static final Pattern ID_PATTERN = Pattern.compile("^@\\S+(\\s\\S*)*$");

    private static final Log LOG = LogFactory.getLog(LineRecordReader.class);

    private static final Pattern SEQUENCE_PATTERN = Pattern.compile("^[ACTGURYSKWMBDHVN\\-\\.]+$");

    private CompressionCodecFactory compressionCodecs = null;

    private Text linesText[] = null;
    private int maxLineLength;

    @Override
    public synchronized void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }

    @Override
    public Text getCurrentKey() {
        return key;
    }

    @Override
    public QueryTemplate getCurrentValue() {
        return value;
    }

    /**
     * Get the progress within the split
     */
    @Override
    public float getProgress() {
        if (splitStart == splitEnd) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - splitStart) / (float) (splitEnd - splitStart));
        }
    }

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
            in = new FastqLineReader(codec.createInputStream(fileIn), conf);
            splitEnd = Long.MAX_VALUE;
        } else {
            if (splitStart != 0) {
                skipFirstLine = true;
                --splitStart;
                fileIn.seek(splitStart);
            }

            in = new FastqLineReader(fileIn, conf);
        }

        if (skipFirstLine) { // skip first line and re-establish "start".
            // Consume the first line.
            splitStart += in.readLine(
                new Text(), 0, (int) Math.min(Integer.MAX_VALUE, splitEnd - splitStart));
        }

        pos = splitStart;
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        final int MAX_FRAME_ADVANCES = 4;

        final int IDX_NAME = 0;
        final int IDX_SEQUENCE = 1;
        final int IDX_COMMENT = 2;
        final int IDX_QUALITY = 3;

        // The size in bytes of the four most recently read lines.
        int thisEntrySize = 0;

        if (key == null) {
            key = new Text();
        }

        if (value == null) {
            value = new QueryTemplate();
        }

        while (pos < splitEnd) {
            // If linesText is null, then we know we still have to find our
            // "reading frame"; otherwise, just read four lines

            if (linesText != null) {
                for (int i = 0; i < 4; i++) {
                    thisEntrySize += in.readLine(
                        linesText[i], maxLineLength,
                        Math.max((int) Math.min(Integer.MAX_VALUE, splitEnd - pos), maxLineLength));
                }
            } else {
                int readSizes[] = new int[4];

                linesText = new Text[4];

                for (int i = 0; i < 4; i++) {
                    linesText[i] = new Text();
                    readSizes[i] = in.readLine(
                        linesText[i], maxLineLength,
                        Math.max((int) Math.min(Integer.MAX_VALUE, splitEnd - pos), maxLineLength));
                }

                // If the lines we read don't fit the expected content pattern,
                // then are most likely in the wrong frame. Move all lines up
                // one index, and re-fill line 4.
                int frameAdvances = -1;
                Text tmp;

                while ((++frameAdvances < MAX_FRAME_ADVANCES) && ( //
                        !ID_PATTERN.matcher(linesText[IDX_NAME].toString()).matches() || //
                                !SEQUENCE_PATTERN.matcher(linesText[IDX_SEQUENCE].toString()).matches() || //
                        !COMMENT_PATTERN.matcher(linesText[IDX_COMMENT].toString()).matches()) //
                ) {
                    // Rotate all indices downward.
                    pos += readSizes[0];
                    tmp = linesText[0];
                    System.arraycopy(readSizes, 1, readSizes, 0, 3);
                    System.arraycopy(linesText, 1, linesText, 0, 3);
                    linesText[3] = tmp;

                    // Read the next line into the last index.
                    readSizes[3] = in.readLine(
                        linesText[3], maxLineLength,
                        Math.max((int) Math.min(Integer.MAX_VALUE, splitEnd - pos), maxLineLength));
                }

                thisEntrySize = readSizes[0] + readSizes[1] + readSizes[2] + readSizes[3];

                if (frameAdvances == MAX_FRAME_ADVANCES) {
                    throw new ReadFormatException(
                        "Unable to match 4 lines to Fastq format. Most recent four lines:" //
                                + "\n  " + linesText[0] //
                                + "\n  " + linesText[1] //
                                + "\n  " + linesText[2] //
                                + "\n  " + linesText[3]);
                }
            }

            if (thisEntrySize == 0) {
                break;
            }

            pos += thisEntrySize;

            if (thisEntrySize < maxLineLength) {
                break;
            }

            // line too long. try again
            LOG.info("Skipped entry of size " + thisEntrySize + " at position "
                    + (pos - thisEntrySize));
        }

        if (thisEntrySize == 0) {
            key = null;
            value = null;
            return false;
        } else {
            int flagsColumn = SequencingRead.FLAG_UNMAPPED;
            int sequenceIdLength;

            value.clear();

            // Strip the sequence identifier of any optional attributes that may
            // be present.

            if (-1 == (sequenceIdLength = linesText[IDX_NAME].find(" "))) {
                sequenceIdLength = linesText[IDX_NAME].getLength();
            }

            // Grab the character after the "/N" suffix in the read identifier.

            if (linesText[IDX_NAME].charAt(sequenceIdLength - 2) == '/') {
                switch (linesText[IDX_NAME].charAt(sequenceIdLength - 1)) {
                case '1':
                    flagsColumn |= SequencingRead.FLAG_FIRST_SEGMENT;
                    flagsColumn |= SequencingRead.FLAG_MULTIPLE_FRAGMENTS;
                    break;
                case '2':
                    flagsColumn |= SequencingRead.FLAG_LAST_SEGMENT;
                    flagsColumn |= SequencingRead.FLAG_MULTIPLE_FRAGMENTS;
                    break;
                default:
                    // The read has a pair member suffix that isn't "/1" or "/2"
                    // (sometimes internal reads will be tagged as /3, for
                    // example). This means it's probably part of a linear
                    // template but is neither the first nor the last fragment.

                    flagsColumn |= SequencingRead.FLAG_FIRST_SEGMENT;
                    flagsColumn |= SequencingRead.FLAG_LAST_SEGMENT;
                    flagsColumn |= SequencingRead.FLAG_MULTIPLE_FRAGMENTS;
                    break;
                }

                value.getTemplateName().set(linesText[IDX_NAME].getBytes(), 1, sequenceIdLength - 3);
            } else {
                // Probably an unpaired read
                value.getTemplateName().set(
                    linesText[IDX_NAME].getBytes(), 1, linesText[IDX_NAME].getLength() - 1);
            }

            SequencingRead read = value.getEmptyRead();

            read.getReadName().set(linesText[IDX_NAME].getBytes(), 1, sequenceIdLength - 1);
            read.setPhred(linesText[IDX_QUALITY]);
            read.setFlags(flagsColumn);
            read.setSequence(
                linesText[IDX_SEQUENCE].getBytes(), 0, linesText[IDX_SEQUENCE].getLength(), true);

            value.add(read);

            key.set(value.getTemplateName());

            return true;
        }
    }

    /*
     * @see
     * edu.cshl.schatz.jnomics.io.JnomicsFileRecordReader#readHeaderData(org
     * .apache.hadoop.fs.Path, org.apache.hadoop.conf.Configuration)
     */
    @Override
    public HeaderData readHeaderData(Path path, Configuration conf) throws IOException {
        HeaderData hd = new HeaderData();
        
        hd.addComment("Original path: " + path.toString());
        
        return hd;
    }

    /**
     * This reads a short read DNA sequence from a text file, one line at a
     * time. TODO
     */
    public static class FastqLineReader extends LineReader {

        /**
         * Create a reader that reads from the given stream using the default
         * buffer-size (64k).
         * 
         * @param in The input stream
         * @throws IOException
         */
        public FastqLineReader(InputStream in) {
            this(in, DEFAULT_BUFFER_SIZE);
        }

        /**
         * Create a reader that reads from the given stream using the
         * <code>io.file.buffer.size</code> specified in the given
         * {@link Configuration}.
         * 
         * @param in The input stream
         * @param conf The configuration
         */
        public FastqLineReader(InputStream in, Configuration conf) {
            this(in, conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE));
        }

        /**
         * Create a line reader that reads from the given stream using the given
         * buffer-size.
         * 
         * @param in The input stream
         * @param bufferSize Size of the read buffer
         * @throws IOException
         */
        public FastqLineReader(InputStream in, int bufferSize) {
            super(in, bufferSize);
        }
    }
}
