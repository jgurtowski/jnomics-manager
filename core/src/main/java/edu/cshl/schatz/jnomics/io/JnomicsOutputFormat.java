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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import edu.cshl.schatz.jnomics.mapreduce.JnomicsJob;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsTool;
import edu.cshl.schatz.jnomics.mapreduce.ReadFileFormat;
import edu.cshl.schatz.jnomics.ob.writable.QueryTemplate;
import edu.cshl.schatz.jnomics.ob.writable.SequencingRead;

/**
 * An {@link OutputFormat} that writes plain text files.
 * 
 * @author Matthew Titmus
 */
/**
 * @author Matthew A. Titmus
 * @param <K>
 * @param <V>
 */
public class JnomicsOutputFormat<K, V> extends FileOutputFormat<K, V> {
    static final Log LOG = LogFactory.getLog(JnomicsTool.class);

    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job)
            throws IOException, InterruptedException {

        final Configuration conf = job.getConfiguration();
        final boolean isCompressed = getCompressOutput(job);

        Class<?> outputValueClass = conf.getClass("mapred.output.value.class", QueryTemplate.class);
  
        if (outputValueClass.isAssignableFrom(QueryTemplate.class)
                || outputValueClass.isAssignableFrom(SequencingRead.class)) {

            CompressionCodec compressionCodec = null;
            ReadFileFormat readFormat = ReadFileFormat.get(conf.get(
                JnomicsJob.P_READ_FORMAT_OUT, "sam"));
            String extension = (readFormat.extension == null) ? "" : "." + readFormat.extension;

            if (isCompressed) {
                Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(
                    job, GzipCodec.class);

                compressionCodec = ReflectionUtils.newInstance(codecClass, conf);
                extension += "." + compressionCodec.getDefaultExtension();
            }

            Path file = getDefaultWorkFile(job, extension);
            FileSystem fs = file.getFileSystem(conf);
            DataOutputStream fileOut = fs.create(file, false);
            RecordWriter<K, V> recordWriter = null;

            if (isCompressed) {
                fileOut = new DataOutputStream(compressionCodec.createOutputStream(fileOut));
            }

            if (outputValueClass.isAssignableFrom(QueryTemplate.class)
                    || outputValueClass.isAssignableFrom(SequencingRead.class)) {
                switch (readFormat) {
                case SAM:
                    recordWriter = new SAMRecordWriter<K, V>(fileOut, conf);
                    break;
                case FASTQ_FLAT:
                    recordWriter = new FlatFastqRecordWriter<K, V>(fileOut, conf);
                    break;
                case BED:
                    recordWriter = new BEDRecordWriter<K, V>(fileOut, conf);
                    break;
                case NONE:
                    throw new UnsupportedOperationException(
                        "Output format is undefined or set to NONE.");
                case FASTQ: // TODO: Figure out how best to write to 2
                            // locations.
                case BAM:
                case FASTA:
                default:
                    throw new UnsupportedOperationException(readFormat
                            + " is not currently supported by JnomicsOutputFormat.");
                }
            }

            return recordWriter;
        } else {
            LOG.info("Output value class is " + outputValueClass
                    + "; returning standard LineRecordWriter");

            return new TextOutputFormat<K, V>().getRecordWriter(job);
        }
    }
}
