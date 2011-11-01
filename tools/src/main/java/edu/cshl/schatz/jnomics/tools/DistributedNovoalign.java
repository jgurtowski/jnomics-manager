/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.tools;

import static edu.cshl.schatz.jnomics.mapreduce.JnomicsJob.P_REFERENCE_INDEX;

import java.io.BufferedInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

import edu.cshl.schatz.jnomics.cli.OptionBuilder;
import edu.cshl.schatz.jnomics.io.FastqRecordWriter;
import edu.cshl.schatz.jnomics.io.SAMRecordReader.SAMLineReader;
import edu.cshl.schatz.jnomics.mapreduce.DistributedBinary;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsTool;
import edu.cshl.schatz.jnomics.ob.writable.QueryTemplate;

/**
 * A MapReduce implementation that wraps Novoalign, providing the capability of
 * distributed read alignment. TODO Implement support for non-paired reads.
 * 
 * @author Matthew Titmus
 */
public class DistributedNovoalign extends DistributedBinary {
    public static final String CMD_DESCRIPTION = "A wrapper class "
            + "around Novoalign (requires Novoalign binary)";

    public static final String CMD_NAME = "novoalign";

    static final String CMD_USAGE = CMD_NAME + " [args] -in <file|dir> -out <dir> -d ";

    static final Log LOG = LogFactory.getLog(DistributedNovoalign.class);

    public DistributedNovoalign() {
        setHelpUsage(CMD_USAGE);
    }

    static void checkExists(File file) throws IOException {
        if (!file.exists()) {
            throw new IOException("File not found: " + file.getCanonicalPath());
        }
    }

    /**
     * A helper method that attempts to retrieve a required configuration
     * property, and throws a{@link IOException} if it doesn't exist.
     * 
     * @param name
     * @return
     * @throws IOException
     */
    static String getReq(Configuration conf, String name) throws IOException {
        String s = conf.get(name);

        if (null == s) {
            throw new IOException("Missing required property: " + name);
        }

        return s;
    }

    public static void main(String[] args) throws Exception {
        JnomicsTool.run(new DistributedNovoalign(), args);
    }

    /**
     * <pre>
     * [-t N] = Quality threshold - nucleotides with lower 
     *          quality will be trimmed (from the end of the sequence).
     * [-l N] = Minimum length - sequences shorter than this (after trimming)
     *          will be discarded. Default = 0 = no minimum length.
     * </pre>
     */
    @Override
    public Options getOptions() {
        Options options = super.getOptions();
        OptionBuilder optionBuilder = new OptionBuilder();

        /**
         * @formatter:off
         */
        options.addOption(optionBuilder
            .withArgName("threads")
            .withDescription("Sets the number of threads to be used. On licensed "
                    + "Novoalign versions defaults to the number of CPU cores. "
                    + "On the free version this option is disabled.")
            .hasArg()
            .isRequired(false)
            .create('c'));

        options.addOption(optionBuilder
            .withArgName("dbname")
            .withDescription("Full pathname of indexed reference sequence from novoindex.")
            .hasArg()
            .isRequired(true)
            .create("d"));

        options.addOption(optionBuilder
            .withDescription("Hard clip trailing bases with quality <= 2")
            .isRequired(false)
            .create("H"));

        options.addOption(optionBuilder
            .withArgName("[mode] 99 99")
            .withDescription("Sets approximate fragment length and standard "
                    + "deviation. Mode is 'MP' for mate pairs and 'PE' "
                    + "for paired end, the mode changes the expected "
                    + "orientation of reads in a proper pair. " + "Default: -i PE 250 30")
            .hasArgs(3)
            .isRequired(false)
            .create("i"));

        options.addOption(optionBuilder
            .withArgName("path")
            .withDescription("Directory for the Novoalign binary. If absent, " 
                    + "then this tool will search the local command path.")
            .hasArg()
            .isRequired(false)
            .create("novo"));

        // No Novoalign equivalent
        options.addOption(optionBuilder
            .withArgName("format [readgroup]")
            .withDescription("Specifies the report format. Native, Pairwise, "
                    + "SAM. Default is Native. Optionally followed by "
                    + "SAM @RG record. Refer to the Novoalign manual "
                    + "for details additional options.")
            .hasArgs(2)
            .isRequired(false)
            .create("o"));

        options.addOption(optionBuilder
            .withArgName("strategy [limit]")
            .withDescription("Sets strategy for reporting repeats. 'None', "
                    + "'Random', 'All', 'Exhaustive', or a posterior "
                    + "probability limit. Default None.")
            .hasArgs(2)
            .isRequired(false)
            .create("r"));

        options.addOption(optionBuilder
            .withArgName("99")
            .withDescription("Sets the maximum alignment score acceptable for the "
                    + "best alignment. Default Automatic. In automatic "
                    + "mode the threshold is set based on read length, "
                    + "genome size and other factors (see novoalign "
                    + "manual for pairs the threshold applies to the "
                    + "fragment and includes both ends and the " + "length penalty.")
            .hasArgs(1)
            .isRequired(false)
            .create("t"));
        /**
         * @formatter:on
         */

        return options;
    }

    /*
     * @see
     * edu.cshl.schatz.jnomics.mapreduce.JnomicsTool#handleOptions(org.apache
     * .commons.cli.CommandLine)
     */
    @Override
    public int handleOptions(CommandLine cmd) throws ParseException, IOException {
        StringBuilder alnArgs = new StringBuilder();
        Configuration conf = getConf();

        for (String unrecognizedOption : cmd.getArgs()) {
            if (unrecognizedOption.startsWith("-")) {
                throw new ParseException("Unrecognized option " + unrecognizedOption);
            }
        }

        // -­c 99
        // Sets the number of threads to be used. On licensed 
        // versions it defaults to the number of CPU cores. On the
        // free version this option is disabled.

        if (cmd.hasOption('c')) {
            alnArgs.append(" -c " + cmd.getOptionValue('c'));
        }

        // -d dbname - Full pathname of indexed reference sequence from
        // novoindex.

        if (cmd.hasOption('d')) {
            conf.set(P_REFERENCE_INDEX, cmd.getOptionValue('d'));
        }

        // -H - Hard clip trailing bases with quality <= 2

        if (cmd.hasOption('H')) {
            alnArgs.append(" -H");
        }

        // -i [mode] 99 99
        // Sets approximate fragment length and standard deviation. Mode
        // is 'MP' for mate pairs and 'PE' for paired end, the mode
        // changes the expected orientation of reads in a proper pair.
        // Default -i PE 250 30

        if (cmd.hasOption('i')) {
            alnArgs.append(" -i");

            for (String val : cmd.getOptionValues('i')) {
                alnArgs.append(" " + val);
            }
        }

        // No Novoalign flag equivalent - Sets the path of the Novoalign
        // binary directory.

        if (null == cmd.getOptionValue("novo")) {
            File binaryDir;
            if (null == (binaryDir = findFile("novoalign"))) {
                throw new FileNotFoundException(
                    "Unable to find the novoalign binary. Please use the -novo parameter.");
            } else {
                getConf().set(P_BINARY_PATH, binaryDir.getParentFile().getCanonicalPath());
            }
        } else {
            conf.set(P_BINARY_PATH, cmd.getOptionValue("novo"));
        }

        // -o format [readgroup]
        // Specifies the report format. Native, Pairwise, SAM. Default
        // is Native. Optionally followed by SAM @RG record. Refer to
        // manual for details additional options.
        if (cmd.hasOption('o')) {
            alnArgs.append(" -o " + cmd.getOptionValue('o'));
        }

        // -r strategy [limit]
        // Sets strategy for reporting repeats. 'None', 'Random', 'All',
        // 'Exhaustive', or a posterior probability limit. Default None.

        if (cmd.hasOption('r')) {
            alnArgs.append(" -r");

            for (String val : cmd.getOptionValues('r')) {
                alnArgs.append(" " + val);
            }
        }

        // Sets the maximum alignment score acceptable for the best
        // alignment. Default Automatic. In automatic mode the
        // threshold is set based on read length, genome size and other
        // factors (see manual). For pairs the threshold applies to the
        // fragment and includes both ends and the length penalty.

        if (cmd.hasOption('t')) {
            alnArgs.append(" -t " + cmd.getOptionValue('t'));
        }

        conf.set(P_BINARY_ARGS, alnArgs.toString().trim());

        return STATUS_OK;
    }

    /*
     * @see
     * edu.cshl.schatz.jnomics.mapreduce.JnomicsTool#run(java.lang.String[])
     */
    @Override
    public int run(String[] args) throws Exception {
        getJob().setReducerClass(DistributedNovoalignReducer.class);
        
        return getJob().waitForCompletion(true) ? 0 : 1;
    }

    /**
     * @author Matthew Titmus
     */
    public static class DistributedNovoalignReducer
            extends JnomicsReducer<Writable, QueryTemplate, Writable, QueryTemplate> {

        private String binaryDirName, referenceIndexName, binaryArgs;
        private File[] fastqFiles;
        private FastqRecordWriter<Writable, QueryTemplate> localFastqOut1, localFastqOut2;
        private int readsInCounter = 0;

        /*
         * @see
         * org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce
         * .Mapper.Context)
         */
        @Override
        public void cleanup(Context context) throws IOException {
            File binaryDirFile, binaryFile, referenceIndexFile;
            Integer processExitValue = null;

            String counterName = this.getClass().getSimpleName();
            context.getCounter(counterName, "Reads in").increment(readsInCounter);

            checkExists(binaryDirFile = new File(binaryDirName));
            checkExists(binaryFile = new File(binaryDirFile, "novoalign"));
            checkExists(referenceIndexFile = new File(referenceIndexName));

            Process process;
            InputStream stderrStream;
            StreamThread stderrThread;

            process = Runtime.getRuntime().exec(
                String.format(
                    "%s -d %s -f %s %s %s", binaryFile.getCanonicalPath(),
                    referenceIndexFile.getCanonicalPath(), fastqFiles[0].getCanonicalPath(),
                    fastqFiles[1].getCanonicalPath(), binaryArgs));

            stderrStream = new BufferedInputStream(process.getErrorStream());
            stderrThread = new StreamThread(stderrStream, System.err);
            stderrThread.start();

            // Novoalign outputs SAM files, which we push into MapReduce. The
            // output stream from the process proper is piped into an
            // InputStream instance.

            int readsKeptCounter = 0, errorsCounter = 0, badReadCounter = 0;

            QueryTemplate queryTemplate = new QueryTemplate();
            SAMLineReader reader = new SAMLineReader(process.getInputStream());

            while (0 != reader.readRecord(queryTemplate)) {
                try {
                    context.write(queryTemplate.getTemplateName(), queryTemplate);

                    readsKeptCounter += queryTemplate.size();
                } catch (Exception e) {
                    LOG.error(e);
                    errorsCounter++;
                }
            }

            context.getCounter(counterName, "Reads out").increment(readsKeptCounter);
            context.getCounter(counterName, "Reads malformed").increment(badReadCounter);
            context.getCounter(counterName, "Reads with errors").increment(errorsCounter);

            // Wait for the process to end, and grab its return value.
            // InterruptedExceptions are rare, but they can be a pain sometimes
            // so be insistent.
            while (processExitValue == null) {
                try {
                    processExitValue = process.waitFor();
                } catch (InterruptedException e) {
                    String msg = String.format(
                        "process.waitFor() interrupted (exec=\"%s\", job_name=%s; job_id=%s).",
                        "novoalign", context.getJobName(), context.getJobID());

                    LOG.warn(msg, e);
                } catch (Exception e) {
                    processExitValue = 1;

                    String msg = String.format(
                        "Exception thrown by process.waitFor() (exec=\"%s\", job_name=%s; job_id=%s).",
                        "novoalign", context.getJobName(), context.getJobID());

                    LOG.error(msg, e);
                }
            }

            if (processExitValue != 0) {
                String msg = String.format(
                    "Process \"%s\" (job_name=%s; job_id=%s) exited with a value of %d.",
                    "novoalign", context.getJobName(), context.getJobID(), processExitValue);

                throw new IOException(msg);
            }
        }

        /*
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
         * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        @Override
        public void reduce(Writable key, Iterable<QueryTemplate> values, Context context)
                throws IOException, InterruptedException {

            for (QueryTemplate queryTemplate : values) {
                localFastqOut1.write(queryTemplate.getTemplateName(), queryTemplate);
                localFastqOut2.write(queryTemplate.getTemplateName(), queryTemplate);
                readsInCounter += 2;
            }
        }

        /*
         * @see
         * org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce
         * .Mapper.Context)
         */
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();

            binaryDirName = getReq(conf, P_BINARY_PATH);
            referenceIndexName = getReq(conf, P_REFERENCE_INDEX);
            binaryArgs = conf.get(P_BINARY_ARGS, "");

            // Create temporary fasta files. We also register them for automatic
            // deletion once the JVM exits, since we don't want to fill up the
            // node's local disks with these (possibly large) files!

            String taskAttemptId = context.getTaskAttemptID().toString();

            fastqFiles = new File[] {
                    new File(taskAttemptId + ".1.fq"),
                    new File(taskAttemptId + ".2.fq") };

            fastqFiles[0].deleteOnExit();
            fastqFiles[1].deleteOnExit();

            localFastqOut1 = new FastqRecordWriter<Writable, QueryTemplate>(
                new DataOutputStream(new FileOutputStream(fastqFiles[0])),
                context.getConfiguration(), FastqRecordWriter.MEMBER_FIRST);
            localFastqOut2 = new FastqRecordWriter<Writable, QueryTemplate>(
                new DataOutputStream(new FileOutputStream(fastqFiles[1])),
                context.getConfiguration(), FastqRecordWriter.MEMBER_LAST);
        }
    }
}
