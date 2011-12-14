/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.io;

import java.io.IOException;

import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMRecordIterator;
import net.sf.samtools.SAMFileReader.ValidationStringency;

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
import edu.cshl.schatz.jnomics.ob.header.HeaderData;
import edu.cshl.schatz.jnomics.ob.writable.QueryTemplate;
import edu.cshl.schatz.jnomics.ob.writable.SequencingRead;
import edu.cshl.schatz.jnomics.ob.writable.SequencingRead.ReadProperties;

/**
 * This record reader reads short-read sequence files, and breaks the data into
 * key/value pairs for input to the Mapper.
 */
public class BAMRecordReader extends JnomicsFileRecordReader {
    static final Log LOG = LogFactory.getLog(BAMRecordReader.class);

    private CompressionCodecFactory compressionCodecs = null;

    private HeaderData headerData;

    private SAMFileReader in;
    private SAMRecord record;
    private SAMRecordIterator recordIterator;

    private static SequencingRead fillRead(SequencingRead sequencingRead, SAMRecord pr) {
        sequencingRead.setCigar(pr.getCigarString());
        sequencingRead.setFlags(pr.getFlags());
        sequencingRead.setMappingPosition(pr.getAlignmentStart());
        sequencingRead.setMappingQuality(pr.getMappingQuality());
        sequencingRead.setNextPosition(pr.getMateAlignmentStart());
        sequencingRead.setNextReferenceName(pr.getMateReferenceName());
        sequencingRead.setOrientation(pr.getReadNegativeStrandFlag()
                ? Orientation.MINUS
                : Orientation.PLUS);
        sequencingRead.setPhred(pr.getBaseQualityString());
        sequencingRead.setReadName(pr.getReadName());
        sequencingRead.setReferenceName(pr.getReferenceName());

        ReadProperties jrp = sequencingRead.getProperties();
        for (SAMRecord.SAMTagAndValue tnv : pr.getAttributes()) {
            jrp.put(tnv.tag, tnv.value.toString());
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

        // TODO Change 'validate' value to false
        sequencingRead.setSequence(pr.getReadString(), true);

        return sequencingRead;
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
        FSDataInputStream fileIn = fs.open(file);

        /**
         * For now, we do NOT split. The ability to split BAM files will be
         * added soon.
         * 
         * <pre>
         * splitStart = split.getStart();
         * splitEnd = splitStart + split.getLength();
         * </pre>
         */
        splitStart = 0;
        splitEnd = Long.MAX_VALUE;

        if (codec != null) {
            in = new SAMFileReader(codec.createInputStream(fileIn));
            splitEnd = Long.MAX_VALUE;
        } else {
            in = new SAMFileReader(fileIn);
        }

        in.setValidationStringency(ValidationStringency.SILENT);
        recordIterator = in.iterator();

        if (!recordIterator.hasNext()) {
            record = null;
            headerData = null;
        } else {
            record = recordIterator.next();
            headerData = new HeaderData(record.getHeader());
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
        if (record != null) {
            String templateName = record.getReadName();

            if (key == null) {
                key = new Text();
            }

            if (value == null) {
                value = new QueryTemplate();
            }

            if (!value.getTemplateNameString().equals(templateName)) {
                // value.clear();
                value.setTemplateLength(Math.abs(record.getInferredInsertSize()));
                value.setTemplateName(record.getReadName());
            }

            value.set(fillRead(value.getEmptyRead(), record));

            while (recordIterator.hasNext()) {
                record = recordIterator.next();

                if (record.getReadName().equals(templateName)) {
                    value.add(fillRead(value.getEmptyRead(), record));
                } else {
                    break;
                }
            }

            if (!recordIterator.hasNext()) {
                record = null;
            }

            key.set(value.getTemplateName());

            return true;
        } else {
            key = null;
            value = null;

            return false;
        }
    }

    /**
     * Grabs the header data for the file on the specified {@link Path} and
     * returns a {@link HeaderData} object (a subclass of Picard's
     * {@link SAMFileHeader}). The default implementation of this method returns
     * <code>null</code>.
     * 
     * @param path The path where the nucleotide sequence file lives.
     * @param conf The {@link Configuration} that describes the filesystem.
     * @return A {@link HeaderData} object, or <code>null</code>.
     * @throws IOException
     */
    @Override
    public HeaderData readHeaderData(Path path, Configuration conf) throws IOException {
        return headerData;
    }
}
