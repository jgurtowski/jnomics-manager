/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.io;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import edu.cshl.schatz.jnomics.mapreduce.JnomicsJob;
import edu.cshl.schatz.jnomics.mapreduce.ReadFileFormat;
import edu.cshl.schatz.jnomics.mapreduce.UnknownReadFileFormatException;
import edu.cshl.schatz.jnomics.ob.writable.QueryTemplate;

/**
 * TODO Comment me.
 */
public class SequencingReadInputFormat extends FileInputFormat<Writable, QueryTemplate> {
    public static final JnomicsFileRecordReader newRecordReader(Path inputFile) {
        ReadFileFormat readFormat = ReadFileFormat.get(inputFile);

        return newRecordReader(readFormat);
    }

    public static final JnomicsFileRecordReader newRecordReader(ReadFileFormat readFormat) {
        JnomicsFileRecordReader recordReader;

        switch (readFormat) {
        case FASTQ_FLAT:
            recordReader = new FlatFastqRecordReader();
            break;
        case SAM:
            recordReader = new SAMRecordReader();
            break;
        case FASTQ:
            recordReader = new FastqRecordReader();
            break;
        case FASTA:
        	recordReader = new FastaRecordReader();
        	break;
        case BAM:
        case BED:
        case NONE:
        default:
            throw new UnsupportedOperationException(readFormat.toString()
                    + " is not yet supported as an input type.");
        }

        return recordReader;
    }

    @Override
    public RecordReader<Writable, QueryTemplate> createRecordReader(InputSplit genericSplit,
        TaskAttemptContext context) throws IOException, InterruptedException {

        FileSplit split = (FileSplit) genericSplit;
        Path file = split.getPath();
        ReadFileFormat readFormat;
        Configuration conf = context.getConfiguration();

        // If the input format is explicitly stated, use that format. If not,
        // attempt to guess it from the file extension.

        if (null != conf.get(JnomicsJob.P_READ_FORMAT_IN)) {
            if (null == (readFormat = ReadFileFormat.get(conf.get(JnomicsJob.P_READ_FORMAT_IN)))) {
                throw new UnknownReadFileFormatException(conf.get(JnomicsJob.P_READ_FORMAT_IN)
                        + " (from " + JnomicsJob.P_READ_FORMAT_IN + ")");
            }
        } else if (null == (readFormat = ReadFileFormat.get(file))) {
            throw new UnknownReadFileFormatException(file.getName());
        }

        return newRecordReader(readFormat);
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        ReadFileFormat readFormat = ReadFileFormat.get(file);

        return (codec == null) && (readFormat != ReadFileFormat.BAM);
    }
}