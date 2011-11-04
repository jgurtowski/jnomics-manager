/**
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

package edu.cshl.schatz.jnomics.io;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import edu.cshl.schatz.jnomics.ob.writable.QueryTemplate;
import edu.cshl.schatz.jnomics.ob.writable.SequencingRead;

/**
 * This record reader reads short-read sequence files, and breaks the data into
 * key/value pairs for input to the Mapper.
 * 
 * @deprecated Use a format-specific reader instead.
 */
@Deprecated
public class SequencingReadRecordReader extends RecordReader<LongWritable, QueryTemplate> {
    /**
     * The {@link Configuration} property used to specify the byte size of the
     * buffer used during input reads.
     */
    public static final String IO_FILE_BUFFER_SIZE = "io.file.buffer.size";

    private static final Log LOG = LogFactory.getLog(SequencingReadRecordReader.class);

    private CompressionCodecFactory compressionCodecs = null;

    /**
     * The reader implementation, itself a wrapper around a {@link LineReader}.
     */
    private SAMSequenceReader in;

    private LongWritable key = null;

    private long pos, splitStart, splitEnd;

    private QueryTemplate value = null;

    /*
     * @see org.apache.hadoop.mapreduce.RecordReader#close()
     */
    @Override
    public synchronized void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }

    /*
     * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentKey()
     */
    @Override
    public LongWritable getCurrentKey() {
        return key;
    }

    /*
     * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentValue()
     */
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
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
        FileSplit split = (FileSplit) genericSplit;
        Configuration job = context.getConfiguration();

        compressionCodecs = new CompressionCodecFactory(job);

        final Path file = split.getPath();
        final CompressionCodec codec = compressionCodecs.getCodec(file);

        splitStart = split.getStart();
        splitEnd = splitStart + split.getLength();

        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(split.getPath());

        if (codec != null) {
            in = new SAMSequenceReader(codec.createInputStream(fileIn), job);
            splitEnd = Long.MAX_VALUE;
        } else {
            /*
             * TODO Header management: 1 - Check if the split's file's headers
             * are in memory; 2 - If not, grab the headers and store them
             * statically; 3 - Advance to "start" or the first non-header line,
             * whichever is greater.
             */

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
                        in = new SAMSequenceReader(fileIn, job);

                        // Always skip the first line, always
                        backPedalPosition += in.readLine(line);

                        // Read until we've caught up to the split start index.
                        while (backPedalPosition < splitStart) {
                            backPedalPosition += in.readRecord(queryTemplate);
                        }
                    } else {
                        LOG.warn("Backpedal reached start of file; bailing out");
                        pos = Long.MAX_VALUE;
                        break;
                    }
                }

                splitStart = backPedalPosition;
            } else {
                in = new SAMSequenceReader(fileIn, job);
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
            key = new LongWritable();
        }

        if (value == null) {
            value = new QueryTemplate();
        }

        // The key is still the byte position in the file (at least for now).
        key.set(pos);

        int recordBytes = 0;

        // Read the next record, and increment the position.
        if (pos <= splitEnd) {
            pos += (recordBytes = in.readRecord(value));
        }

        if (recordBytes == 0) {
            key = null;
            value = null;

            return false;
        } else {
            return true;
        }
    }

    /**
     * This reads a short read DNA sequence from a text file, one line at a
     * time.
     */
    class SAMSequenceReader extends LineReader {
        private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

        private static final int MAX_BYTES_TO_CONSUME = Integer.MAX_VALUE;

        private static final int MAX_LINE_LENGTH = Integer.MAX_VALUE;

        private static final byte TAB = '\t';

        private Text column = new Text(), propertyKey = new Text(), propertyValue = new Text();

        private Text line = new Text();

        /**
         * The number of bytes consumed in the course of a read-ahead.
         */
        private long readAheadBytes = 0;

        /**
         * When a record is read, it is first stored here. If it is part of the
         * current template's record set, it is returned immediately; otherwise
         * it is held until the next
         * {@link SAMSequenceReader#readRecord(QueryTemplate)} call.
         */
        private SequencingRead readAheadRecord = null;

        /**
         * The read-ahead query template's total length.
         */
        private int readAheadTemplateLength = 0;

        /**
         * Create a reader that reads from the given stream using the default
         * buffer-size (64k).
         * 
         * @param in The input stream
         * @throws IOException
         */
        public SAMSequenceReader(InputStream in) {
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
        public SAMSequenceReader(InputStream in, Configuration conf) {
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
        public SAMSequenceReader(InputStream in, int bufferSize) {
            super(in, bufferSize);
        }

        /**
         * @return The number of bytes advanced, including the terminal newline.
         * @throws IOException
         */
        public long advanceReadPointer() throws IOException {
            long bytes = 0;

            bytes += readLine(line);

            System.out.println(line);

            bytes += readRecord(new QueryTemplate());

            return bytes;
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
         */
        public int readRecord(QueryTemplate queryTemplate) throws IOException {
            final int MAX_LINE_LENGTH = Integer.MAX_VALUE;
            final int MAX_BYTES_TO_CONSUME = Integer.MAX_VALUE;

            long bytesConsumed = 0, lineBytes;
            boolean newTemplate = true;

            queryTemplate.clear();

            // If we have a record present from a previous read, use that to
            // start of this one.

            if (readAheadRecord != null) {
                queryTemplate.setTemplateLength(readAheadTemplateLength);
                queryTemplate.setTemplateName(readAheadRecord.getReadName());
                queryTemplate.add(readAheadRecord);

                bytesConsumed += readAheadBytes;

                readAheadRecord = null;
                readAheadBytes = 0;
                readAheadTemplateLength = 0;

                newTemplate = false;
            }

            // If we're ready for a new record, then we read exactly one line,
            // without regard to line length to number of bytes consumed.

            while ((readAheadRecord == null)
                    && (0 < (lineBytes = readLine(line, MAX_LINE_LENGTH, MAX_BYTES_TO_CONSUME)))) {

                readAheadBytes += lineBytes;

                if (line.charAt(0) == '@') {
                    // TODO Handle header line
                    continue;
                } else {
                    int readFrom = 0;

                    readAheadRecord = new SequencingRead();

                    // Read the SAM columns one at a time.

                    // Line 1: Query template name (also used as the read name)
                    readFrom = getColumn(line, column, readFrom);
                    readAheadRecord.setReadName(column);

                    // Line 2: Bitwise flag
                    readFrom = getColumn(line, column, readFrom);
                    readAheadRecord.setFlags(Integer.parseInt(column.toString()));

                    // Line 3: Reference sequence name
                    readFrom = getColumn(line, column, readFrom);
                    readAheadRecord.setReferenceName(column);

                    // Line 4: 1-based leftmost mapping position
                    readFrom = getColumn(line, column, readFrom);
                    readAheadRecord.reposition(Integer.parseInt(column.toString()));

                    // Line 5: Mapping quality
                    readFrom = getColumn(line, column, readFrom);
                    readAheadRecord.setMappingQuality(Integer.parseInt(column.toString()));

                    // Line 6: CIGAR String
                    readFrom = getColumn(line, column, readFrom);
                    readAheadRecord.setCigar(column);

                    // Line 7: Reference name of mate/next fragment.
                    readFrom = getColumn(line, column, readFrom);
                    readAheadRecord.setNextReferenceName(column);

                    // Line 8: Position of mate/next fragment.
                    readFrom = getColumn(line, column, readFrom);
                    readAheadRecord.setNextPosition(Integer.parseInt(column.toString()));

                    // Line 9: Observed template length.
                    readFrom = getColumn(line, column, readFrom);
                    readAheadTemplateLength = Math.abs(Integer.parseInt(column.toString()));

                    // Line 10: Fragment sequence
                    readFrom = getColumn(line, column, readFrom);
                    readAheadRecord.setSequence(column.getBytes(), 0, column.getLength(), false);

                    // Line 11: ASCII-encoded Phred quality string
                    readFrom = getColumn(line, column, readFrom);
                    readAheadRecord.setPhred(column);

                    // Line 12: Optional tags
                    readAheadRecord.getProperties().clear();
                    while (readFrom != -1) {
                        readFrom = getColumn(line, column, readFrom);

                        // TODO Make this (much) more efficient.
                        String tag[] = column.toString().split(":");

                        propertyKey.set(tag[0] + ":" + tag[1]);
                        propertyValue.set(tag[2]);
                        readAheadRecord.getProperties().put(propertyKey, propertyValue);
                    }

                    // If this is the first iteration of the loop, or if the
                    // read name indicates that it belongs to this query
                    // template, add the read to the template and keep going.

                    if (newTemplate) {
                        queryTemplate.setTemplateLength(readAheadTemplateLength);
                        queryTemplate.setTemplateName(readAheadRecord.getReadName());
                    } else if (!queryTemplate.getTemplateNameString().equals(
                        readAheadRecord.getReadName())) {
                        break;
                    }

                    queryTemplate.add(readAheadRecord);

                    bytesConsumed += readAheadBytes;
                    readAheadBytes = 0;
                    readAheadRecord = null;

                    newTemplate = false;
                }
            }

            return (int) bytesConsumed;
        }

        /**
         * Reads up to and through the next valid line from the InputStream into
         * the given {@link SequencingRead}. If no valid line can be found, the
         * method will return without an exception, and the contents of
         * <code>sequencingRead</code>
         * 
         * @param sequencingRead The object to store the next record.
         * @return The number of bytes read, including any control characters.
         * @throws IOException If thrown by the underlying stream.
         */
        public int readRecord(SequencingRead sequencingRead) throws IOException {
            long bytesConsumed = 0;

            if (readAheadRecord == null) {
                long lineBytes = 0;
                boolean foundRecord = false;

                // This is probably redundant, since all the fields are
                // overwritten during the read. If not, uncomment this.
                //
                // sequencingRead.clear();

                // Read exactly one line at a time, without regard to line
                // length, and stop when a record is found or the end of the
                // file is reached.
                while (!foundRecord
                        || (0 < (lineBytes = readLine(line, MAX_LINE_LENGTH, MAX_BYTES_TO_CONSUME)))) {

                    bytesConsumed += lineBytes;

                    foundRecord = parseSequencingReadText(sequencingRead, line);
                }

                if (!foundRecord) {
                    sequencingRead.clear();
                }
            } else {
                sequencingRead.set(readAheadRecord);
                bytesConsumed = readAheadBytes;

                readAheadBytes = 0;
                readAheadRecord = null;
                readAheadTemplateLength = 0;
            }

            return (int) bytesConsumed;
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
        boolean parseSequencingReadText(SequencingRead sequencingRead, Text line) {
            boolean foundRecord = false;

            if (line.charAt(0) != '@') {
                int positionPointer = 0;
                Text column = new Text();

                // Read the SAM columns one at a time.

                // Line 1: Query template name (also used as the read name)
                positionPointer = getColumn(line, column, positionPointer);
                sequencingRead.setReadName(column);

                // Line 2: Bitwise flag
                positionPointer = getColumn(line, column, positionPointer);
                sequencingRead.setFlags(Integer.parseInt(column.toString()));

                // Line 3: Reference sequence name
                positionPointer = getColumn(line, column, positionPointer);
                sequencingRead.setReferenceName(column);

                // Line 4: 1-based leftmost mapping position
                positionPointer = getColumn(line, column, positionPointer);
                sequencingRead.reposition(Integer.parseInt(column.toString()));

                // Line 5: Mapping quality
                positionPointer = getColumn(line, column, positionPointer);
                sequencingRead.setMappingQuality(Integer.parseInt(column.toString()));

                // Line 6: CIGAR String
                positionPointer = getColumn(line, column, positionPointer);
                sequencingRead.setCigar(column);

                // Line 7: Reference name of mate/next fragment.
                positionPointer = getColumn(line, column, positionPointer);
                sequencingRead.setNextReferenceName(column);

                // Line 8: Position of mate/next fragment.
                positionPointer = getColumn(line, column, positionPointer);
                sequencingRead.setNextPosition(Integer.parseInt(column.toString()));

                // Line 9: Observed template length.
                positionPointer = getColumn(line, column, positionPointer);
                Math.abs(Integer.parseInt(column.toString()));

                // Line 10: Fragment sequence
                positionPointer = getColumn(line, column, positionPointer);
                sequencingRead.setSequence(column.getBytes(), 0, column.getLength(), false);

                // Line 11: ASCII-encoded Phred quality string
                positionPointer = getColumn(line, column, positionPointer);
                sequencingRead.setPhred(column);

                // Line 12: Optional tags
                sequencingRead.getProperties().clear();
                while (positionPointer != -1) {
                    positionPointer = getColumn(line, column, positionPointer);

                    String[] tag = column.toString().split(":");

                    propertyKey.set(tag[0] + ":" + tag[1]);
                    propertyValue.set(tag[2]);
                    sequencingRead.getProperties().put(propertyKey, propertyValue);
                }
            }

            return foundRecord;
        }

        /**
         * Read characters from an (assumedly) tab-delimited string contained in
         * a {@link Text} object, starting at a specified position and
         * continuing until a <code>\t</code> </pre> character is reached.
         * 
         * @param readFrom The {@link Text} object to read from.
         * @param writeTo A {@link Text} object to which the contents of the
         *            next column are written.
         * @param startIndex The index to start seeking from. If the input is
         *            <code>&lt;0</code>, <code>writeTo</code> is simply cleared
         *            and the method returns <code>-1</code>.
         * @return Index of the start position of the next column, or
         *         <code>-1</code> if the end of the line has been reached.
         */
        private int getColumn(Text readFrom, Text writeTo, int startPosition) {
            writeTo.clear();

            if (startPosition >= 0) {
                byte[] bytes = readFrom.getBytes();
                int length = readFrom.getLength(), pos;

                for (pos = startPosition; pos < length; pos++) {
                    if (bytes[pos] == TAB) {
                        break;
                    }
                }

                writeTo.set(bytes, startPosition, pos - startPosition);

                return (pos == length ? -1 : pos + 1);
            } else {
                return -1;
            }
        }
    }
}
