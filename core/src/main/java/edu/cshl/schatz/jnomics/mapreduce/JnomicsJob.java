/*
 * Copyright (C) 2011 Matthew A. Titmus
 */

package edu.cshl.schatz.jnomics.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapreduce.Job;

import edu.cshl.schatz.jnomics.io.JnomicsFileRecordReader;
import edu.cshl.schatz.jnomics.io.JnomicsOutputFormat;
import edu.cshl.schatz.jnomics.io.SequencingReadInputFormat;
import edu.cshl.schatz.jnomics.ob.writable.QueryTemplate;

/**
 * @author Matthew Titmus
 */
public class JnomicsJob extends Job {
    /**
     * A {@link Configuration} property: Specifies whether the mapper throw an
     * exception if it receives zero reads? Default = <code>true</code>.
     */
    public static final String P_MAP_FAIL_IF_NO_READS_IN = "jnomics.map.fail.no-reads.in";

    /**
     * A {@link Configuration} property: Specifies whether the mapper throw an
     * exception if it commits zero reads. Default = <code>false</code>.
     */
    public static final String P_MAP_FAIL_IF_NO_READS_OUT = "jnomics.map.fail.no-reads.out";

    /**
     * A {@link Configuration} property: Specifies the expected read format of
     * the source file(s).
     */
    public static final String P_READ_FORMAT_IN = "jnomics.reads.format.in";

    /**
     * A {@link Configuration} property: Specifies the format in which to write
     * the final output file(s). Default = <code>sam</code>.
     */
    public static final String P_READ_FORMAT_OUT = "jnomics.reads.format.out";

    /**
     * A {@link Configuration} property: Specified whether the reducer should
     * throw an exception if it receives zero reads? Default =
     * <code>false</code>.
     */
    public static final String P_REDUCE_FAIL_IF_NO_READS_IN = "jnomics.reduce.fail.no-reads.in";

    /**
     * A {@link Configuration} property: If the job requires a reference genome,
     * it is specified in this property.
     */
    public static final String P_REFERENCE_GENOME = "jnomics.aligner.reference.genome";

    /**
     * A {@link Configuration} property: If the job requires an indexed
     * reference, it is specified in this property.
     */
    public static final String P_REFERENCE_INDEX = "jnomics.aligner.indexfile";

    /**
     * A {@link Configuration} property: Specifies the value to be passed to
     * {@link Job#waitForCompletion(boolean)} when a verbose flag isn't
     * specified. Default=<code>false</code>.
     */
    public static final String P_VERBOSE = "jnomics.verbose";

    /**
     * Builds a new job client using a default {@link Configuration}, and
     * connects to the default {@link JobTracker}.
     * 
     * @see Configuration#Configuration(boolean)
     * @throws IOException if there is an exception thrown while connecting to
     *             the default {@link JobTracker}.
     */
    public JnomicsJob() throws IOException {
        super();

        setDefaults();
    }

    /**
     * Builds a new job client with the given {@link Configuration}, and
     * connects to the default {@link JobTracker}.
     * 
     * @param conf a user-provided {@link Configuration}.
     * @throws IOException if there is an exception thrown while connecting to
     *             the default {@link JobTracker}.
     */
    public JnomicsJob(Configuration conf) throws IOException {
        super(conf);

        setDefaults();
    }

    /**
     * Builds a new job client with the given {@link Configuration} and name,
     * and connects to the default {@link JobTracker}.
     * 
     * @param conf a user-provided {@link Configuration}.
     * @param jobName the name to assign the job.
     * @throws IOException if there is an exception thrown while connecting to
     *             the default {@link JobTracker}.
     */
    public JnomicsJob(Configuration conf, String jobName) throws IOException {
        super(conf, jobName);
    }

    /**
     * Returns the contents of the {@link #P_READ_FORMAT_IN} configuration
     * property.
     * 
     * @return The value of
     *         <code>getConfiguration().get({@link #P_READ_FORMAT_IN})</code>
     */
    public String getSequencingReadInputFormat() {
        return conf.get(P_READ_FORMAT_IN);
    }

    /**
     * Returns the contents of the {@link #P_READ_FORMAT_OUT} configuration
     * property.
     * 
     * @return The value of
     *         <code>getConfiguration().get({@link #P_READ_FORMAT_OUT})</code>
     */
    public String getSequencingReadOutputFormat() {
        return conf.get(P_READ_FORMAT_OUT);
    }

    /**
     * Instructs {@link SequencingReadInputFormat} to ignore input file
     * extensions when determining the input file format ("sam", "fastq", "bed",
     * etc) for the purpose of providing a format-specific
     * {@link JnomicsFileRecordReader} implementation, and to instead assume
     * that the input has format specified.
     * 
     * @param format A {@link ReadFileFormat} value.
     */
    public void setSequencingReadInputFormat(ReadFileFormat format) {
        conf.set(P_READ_FORMAT_IN, format.toString());
    }

    /**
     * Instructs {@link SequencingReadInputFormat} to ignore input file
     * extensions when determining the input file format ("sam", "fastq", "bed",
     * etc) for the purpose of providing a format-specific
     * {@link JnomicsFileRecordReader} implementation, and to instead assume
     * that the input has format specified.
     * <p>
     * TODO Add support for custom input formats.
     * 
     * @throws UnknownReadFileFormatException if the format name is unknown.
     */
    public void setSequencingReadInputFormat(String format) throws UnknownReadFileFormatException {
        ReadFileFormat inputSequenceFormat;

        if (null == (inputSequenceFormat = ReadFileFormat.get(format))) {
            throw new UnknownReadFileFormatException(format);
        } else {
            setSequencingReadInputFormat(inputSequenceFormat);
        }
    }

    /**
     * A convenience method that allows the sequencing read output format to be
     * set using a {@link ReadFileFormat} enum.
     */
    public void setSequencingReadOutputFormat(ReadFileFormat outputSequenceFormat) {
        conf.set(P_READ_FORMAT_OUT, outputSequenceFormat.toString());
    }

    /**
     * The desired input sequencing read format ("sam", "fastq", "bed", etc).
     * TODO Add support for custom sequencing read codecs.
     * 
     * @throws UnknownReadFileFormatException
     */
    public void setSequencingReadOutputFormat(String outputSequenceFormatName)
            throws UnknownReadFileFormatException {

        ReadFileFormat outputSequenceFormat;

        if (null == (outputSequenceFormat = ReadFileFormat.get(outputSequenceFormatName))) {
            throw new UnknownReadFileFormatException(outputSequenceFormatName);
        } else {
            setSequencingReadOutputFormat(outputSequenceFormat);
        }
    }

    /**
     * Submit the job to the cluster and wait for it to finish. Rather than
     * having the user specify a <code>verbose</code> value, this method passes
     * the (boolean) configuration value of "jnomics.verbose".
     * 
     * @return true if the job succeeded
     * @throws IOException thrown if the communication with the
     *             <code>JobTracker</code> is lost
     * @see Job#waitForCompletion(boolean)
     */
    public boolean waitForCompletion()
            throws IOException, InterruptedException, ClassNotFoundException {

        return waitForCompletion(getConfiguration().getBoolean(P_VERBOSE, false));
    }

    /**
     * Sets the default task name to the class that created this instance.
     */
    private void setDefaultName() {
        final String ignorePackage = getClass().getPackage().getName();
        StackTraceElement[] ste = new Exception().fillInStackTrace().getStackTrace();

        for (StackTraceElement e : ste) {
            String className = e.getClassName();

            if (!className.startsWith(ignorePackage)) {
                setJobName(className);

                try {
                    setJarByClass(Class.forName(className));
                } catch (ClassNotFoundException e1) { /* No-op */}

                break;
            }
        }
    }

    private void setDefaults() {
        Configuration conf = getConfiguration();

        conf.setBoolean("jnomics.job", true);
        conf.set(P_READ_FORMAT_OUT, "sam");

        setMapperClass(JnomicsMapper.class);
        setReducerClass(JnomicsReducer.class);

        setInputFormatClass(SequencingReadInputFormat.class);
        setOutputFormatClass(JnomicsOutputFormat.class);

        setOutputKeyClass(Text.class);
        setOutputValueClass(QueryTemplate.class);

        setDefaultName();
    }
}
