/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.io;

import static edu.cshl.schatz.jnomics.ob.writable.SequencingRead.FLAG_REVERSE_COMPLEMENTED;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileReader;

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

import edu.cshl.schatz.jnomics.ob.Orientation;
import edu.cshl.schatz.jnomics.ob.writable.QueryTemplate;
import edu.cshl.schatz.jnomics.ob.writable.SequencingRead;
import edu.cshl.schatz.jnomics.util.TextCutter;
import edu.cshl.schatz.jnomics.util.TextUtil;

/**
 * This record reader reads short-read sequence files, and breaks the data into
 * key/value pairs for input to the Mapper.
 */
public class SAMRecordReader extends JnomicsFileRecordReader {
    private static final Log LOG = LogFactory.getLog(SAMRecordReader.class);

    private static HashMap<String, SAMFileHeader> samFileHeaders = new HashMap<String, SAMFileHeader>();

    private CompressionCodecFactory compressionCodecs = null;

    /**
     * TODO Figure out where and how to store headers.
     */
    private SAMFileHeader splitHeaders;

    /**
     * Grabs the header data for the SAM file on the specified Path.
     * 
     * @param path
     * @param fs
     * @return
     * @throws IOException
     */
    private static final SAMFileHeader getHeaderData(Path path, FileSystem fs) throws IOException {
        SAMFileHeader samFileHeader = null;
        String qualifiedUri = path.makeQualified(fs).toUri().toString();

        synchronized (samFileHeaders) {
            if (null == (samFileHeader = samFileHeaders.get(qualifiedUri))) {
                SAMFileReader reader = new SAMFileReader(fs.open(path));

                samFileHeader = reader.getFileHeader();
                samFileHeaders.put(qualifiedUri, samFileHeader);
            }
        }

        return samFileHeader;
    }

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
        FileSystem fs = file.getFileSystem(conf);

        FSDataInputStream fileIn = fs.open(split.getPath());

        splitStart = split.getStart();
        splitEnd = splitStart + split.getLength();

        try {
            splitHeaders = getHeaderData(file, fs);
        } catch (IllegalArgumentException e) {
            LOG.warn(e);
        }

        if (codec != null) {
            in = new SAMLineReader(codec.createInputStream(fileIn), conf);
            splitEnd = Long.MAX_VALUE;
        } else {
            // If we're not at the first line, track back to the last line, then
            // read ahead until we find the first set of records that begin
            // after the split start.

            if (splitStart != 0) {
                final long backpedalStep = 1024;

                Text line = new Text();
                QueryTemplate queryTemplate = new QueryTemplate();
                long backPedalBytes = 0;
                long backPedalPosition = 0;

                while (queryTemplate.size() == 0) {
                    if (0 < (backPedalPosition = splitStart - (backPedalBytes += backpedalStep))) {
                        fileIn.seek(backPedalPosition);
                        in = new SAMLineReader(fileIn, conf);

                        // Always skip the first line, always
                        backPedalPosition += in.readLine(line);

                        // Read until we've caught up to the split start index.
                        while (backPedalPosition < splitStart) {
                            backPedalPosition += ((SAMLineReader) in).readRecord(queryTemplate);
                        }
                    } else {
                        LOG.warn("Backpedal reached start of file; bailing out");
                        pos = Long.MAX_VALUE;
                        return;
                    }
                }

                splitStart = backPedalPosition;
            } else {
                in = new SAMLineReader(fileIn, conf);
            }
        }

        pos = splitStart;
    }

    /**
     * Tries to grab the next set of fragment entries for the same query
     * template.
     * 
     * @see org.apache.hadoop.mapreduce.RecordReader#nextKeyValue()
     */
    @Override
    public boolean nextKeyValue() throws IOException {
        if (key == null) {
            key = new Text();
        }

        if (value == null) {
            value = new QueryTemplate();
        }

        long recordBytes = 0;

        // Read the next record, and increment the position.
        if (pos <= splitEnd) {
            pos += (recordBytes = ((SAMLineReader) in).readRecord(value));
        }

        if (recordBytes == 0) {
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
    public static class SAMLineReader extends RecordLineReader {
        private TextCutter colonCutter = new TextCutter().setDelimiter(':');

        private Text line = new Text();

        /**
         * When a record is read, it is first stored here. If it is part of the
         * current template's record set, it is returned immediately; otherwise
         * it is held until the next
         * {@link SAMLineReader#readRecord(QueryTemplate)} call.
         */
        private SequencingRead read = null;

        /**
         * The number of bytes consumed in the course of a read-ahead.
         */
        private long readAheadBytes = 0;

        /**
         * The read-ahead query template's total length.
         */
        private int readAheadTemplateLength = 0;

        private TextCutter tabCutter = new TextCutter().setDelimiter('\t');

        /**
         * Create a reader that reads from the given stream using the default
         * buffer-size (64k).
         * 
         * @param in The input stream
         * @throws IOException
         */
        public SAMLineReader(InputStream in) {
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
        public SAMLineReader(InputStream in, Configuration conf) {
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
        public SAMLineReader(InputStream in, int bufferSize) {
            super(in, bufferSize);
        }

        /**
         * Reads a single-line record from characters contained in a
         * {@link Text} object and fills a {@link SequencingRead} instance.
         * 
         * @param sequencingRead The object to store the record into.
         * @param line The {@link Text} instance to read from.
         * @return <code>True</code> if <code>line</code> contained a valid
         *         record and <code>sequencingRead</code> was filled.
         */
        public boolean parseLine(SequencingRead sequencingRead, Text line) {
            if (line.charAt(0) == '@') {
                sequencingRead.clear();
                return false;
            } else if (tabCutter.set(line).getCutCount() < 11) {
                LOG.warn("Invalid SAM line: " + line.toString());
                sequencingRead.clear();
                return false;
            } else {
                // Read the SAM columns one at a time.
                // Line 1: Query template name (also used as the read name)
                sequencingRead.setReadName(tabCutter.getCut(0));

                // Line 2: Bitwise flag
                sequencingRead.setFlags(Integer.parseInt(tabCutter.getCut(1).toString()));

                // Line 3: Reference sequence name
                sequencingRead.setReferenceName(tabCutter.getCut(2));

                // Line 4: 1-based leftmost mapping position
                sequencingRead.reposition(Integer.parseInt(tabCutter.getCut(3).toString()));

                // Line 5: Mapping quality
                sequencingRead.setMappingQuality(Integer.parseInt(tabCutter.getCut(4).toString()));

                // Line 6: CIGAR String
                sequencingRead.setCigar(tabCutter.getCut(5));

                // Line 7: Reference name of mate/next fragment.
                sequencingRead.setNextReferenceName(tabCutter.getCut(6));

                // Line 8: Position of mate/next fragment.
                sequencingRead.setNextPosition(Integer.parseInt(tabCutter.getCut(7).toString()));

                // Line 9: Observed template length.
                readAheadTemplateLength = Math.abs(Integer.parseInt(tabCutter.getCut(8).toString()));

                // Line 10: Fragment sequence
                Text seq = tabCutter.getCut(9);
                sequencingRead.setSequence(seq.getBytes(), 0, seq.getLength(), false);

                // Line 11: ASCII-encoded Phred quality string
                sequencingRead.setPhred(tabCutter.getCut(10));

                // Line 12: Optional tags
                sequencingRead.getProperties().clear();
                for (int i = 11; i < tabCutter.getCutCount(); i++) {
                    colonCutter.set(tabCutter.getCut(i));

                    Text key = new Text(colonCutter.getCutRange(0, 1));
                    Text value = new Text(colonCutter.getCut(2));

                    sequencingRead.getProperties().put(key, value);
                }

                // Line input complete: finalize attributes according to
                // recently input values.

                if (!hasTrailingSlashNumber(sequencingRead.getReadName())) {
                    if (sequencingRead.isFirst()) {
                        sequencingRead.getReadName().append(SLASH_FIRST, 0, 2);
                    } else {
                        sequencingRead.getReadName().append(SLASH_LAST, 0, 2);
                    }
                }

                sequencingRead.setOrientation(sequencingRead.hasFlag(FLAG_REVERSE_COMPLEMENTED)
                        ? Orientation.MINUS
                        : Orientation.PLUS);

                return true;
            }
        }

        @Override
        public int readRecord(QueryTemplate queryTemplate) throws IOException {
            return readRecord(queryTemplate, Integer.MAX_VALUE, Integer.MAX_VALUE);
        }

        /**
         * Reads one or more lines from the InputStream into the given
         * {@link QueryTemplate}. A line can be terminated by one of the
         * following: '\n' (LF) , '\r' (CR), or '\r\n' (CR+LF). EOF also
         * terminates an otherwise unterminated line.
         * 
         * @param temp the object to store the next one or more records.
         * @return the number of bytes read including the (longest) newline
         *         found.
         * @throws IOException if thrown by the underlying stream.
         */
        @Override
        public int readRecord(QueryTemplate temp, int maxLineLength, int maxBytesToConsume)
                throws IOException {
            long bytesConsumed = 0, lineBytes;
            boolean newTemplate = true;

            temp.clear();

            // If we have a record present from a previous read, use that to
            // start of this one.

            if (read != null) {
                temp.setTemplateLength(readAheadTemplateLength);
                temp.setTemplateName(read.getReadName());
                temp.add(read);

                // Per internal Jnomics standard, sequencing read names should
                // have trailing "/[0-9]" suffixes, and query template names
                // should not.

                trimTrailingSlashNumber(temp.getTemplateName());

                bytesConsumed += readAheadBytes;

                read = null;
                readAheadBytes = 0;
                readAheadTemplateLength = 0;

                newTemplate = false;
            }

            // If we're ready for a new record, then we read exactly one line,
            // without regard to line length to number of bytes consumed. If
            // that line turns out to be a header, we skip it.

            while ((read == null)
                    && (0 < (lineBytes = readLine(line, maxLineLength, maxBytesToConsume)))) {

                readAheadBytes += lineBytes;
                read = temp.getEmptyRead();

                if (parseLine(read, line)) {
                    // If this is the first iteration of the loop, or if the
                    // read name indicates that it belongs to this query
                    // template, add the read to the template and keep going.

                    if (newTemplate) {
                        temp.setTemplateLength(readAheadTemplateLength);
                        temp.setTemplateName(read.getReadName());

                        trimTrailingSlashNumber(temp.getTemplateName());
                    } else if (!TextUtil.startsWith(read.getReadName(), temp.getTemplateName())) {
                        break;
                    }

                    temp.add(read);

                    bytesConsumed += readAheadBytes;
                    readAheadBytes = 0;
                    read = null;

                    newTemplate = false;
                } else {
                    read = null;
                }
            }

            return (int) bytesConsumed;
        }
    }
}
