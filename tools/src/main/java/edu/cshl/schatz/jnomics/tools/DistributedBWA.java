/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.tools;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducerO;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

import edu.cshl.schatz.jnomics.cli.OptionBuilder;
import edu.cshl.schatz.jnomics.cli.WeightedOption;
import edu.cshl.schatz.jnomics.io.FastqRecordWriter;
import edu.cshl.schatz.jnomics.io.SAMRecordReader.SAMLineReader;
import edu.cshl.schatz.jnomics.mapreduce.DistributedBinary;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsJob;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsTool;
import edu.cshl.schatz.jnomics.ob.writable.QueryTemplate;

/**
 * A Jnomics implementation that distributes a BWA alignment by splitting the
 * effort among the nodes. This is performed during the reduce step rather than
 * the map, which allows the shuffle to merge reads from the same template that
 * may not have been associated during the initial read (this can happen if the
 * input file isn't sorted by name or the reads are in different files [typical
 * of FASTQ format]).
 * 
 * @author Matthew Titmus
 */
public class DistributedBWA extends DistributedBinary {
    public static final String CMD_DESCRIPTION = "Perform a BWA "
            + "aln+sampe in MapReduce (requires BWA binary)";

    public static final String CMD_NAME = "bwa";

    public static final String P_BINARY_ALN_ARGS = P_BINARY_ARGS + ".aln";

    public static final String P_BINARY_SAMXE_ARGS = P_BINARY_ARGS + ".samxe";

    static final String CMD_HEADING = CMD_DESCRIPTION;

    static final String CMD_USAGE = CMD_NAME + " [options] <in.db.fasta>";

    static final Log LOG = LogFactory.getLog(DistributedBWA.class);

    static final String OPT_ARGS_BWA = "aa";

    static final String OPT_ARGS_SAMPE = "as";

    static final String OPT_FASTA_DB = "db";

    public DistributedBWA() {
        setHelpUsage(CMD_USAGE);
        setHelpHeading(CMD_HEADING);
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
        System.exit(JnomicsTool.run(new DistributedBWA(), args));
    }

    /**
     * <pre>
     * Usage:   bwa aln [options] <prefix> <in.fq>
     * 
     * Options: -n NUM    max #diff (int) or missing prob under 0.02 err rate (float) [0.04]
     *          -o INT    maximum number or fraction of gap opens [1]
     *          -e INT    maximum number of gap extensions, -1 for disabling long gaps [-1]
     *          -i INT    do not put an indel within INT bp towards the ends [5]
     *          -d INT    maximum occurrences for extending a long deletion [10]
     *          -l INT    seed length [32]
     *          -k INT    maximum differences in the seed [2]
     *          -m INT    maximum entries in the queue [2000000]
     *          -t INT    number of threads [1]
     *          -M INT    mismatch penalty [3]
     *          -O INT    gap open penalty [11]
     *          -E INT    gap extension penalty [4]
     *          -R INT    stop searching when there are >INT equally best hits [30]
     *          -q INT    quality threshold for read trimming down to 35bp [0]
     *          -f FILE   file to write output to instead of stdout
     *          -B INT    length of barcode
     *          -c        input sequences are in the color space
     *          -L        log-scaled gap penalty for long deletions
     *          -N        non-iterative mode: search for all n-difference hits (slooow)
     *          -I        the input is in the Illumina 1.3+ FASTQ-like format
     *          -b        the input read file is in the BAM format
     *          -0        use single-end reads only (effective with -b)
     *          -1        use the 1st read in a pair (effective with -b)
     *          -2        use the 2nd read in a pair (effective with -b)
     * </pre>
     */
    @Override
    public Options getOptions() {
        final int weight = WeightedOption.DEFAULT_WEIGHT;

        OptionBuilder optionBuilder = new OptionBuilder();
        Options options = new Options();

        /** @formatter:off */
        options.addOption(optionBuilder
            .withArgName("db.fa")
            .withDescription("The FASTA-formatted db file. Required.")
            .hasArg()
            .isRequired(true)
            .withWeight(weight + 5)
            .withLongOpt("fasta-db")
            .create(OPT_FASTA_DB)
            );

        options.addOption(optionBuilder
            .withArgName("args")
            .withDescription("Parameters to pass to the 'bwa aln' command (enclose in quotes).")
            .hasArg()
            .isRequired(false)
            .withLongOpt("aln-args")
            .create(OPT_ARGS_BWA)
            );

        options.addOption(optionBuilder
            .withArgName("args")
            .withDescription("Parameters to pass to the 'bwa sampe' command (enclose in quotes).")
            .hasArg()
            .isRequired(false)
            .withLongOpt("sampe-args")
            .create(OPT_ARGS_SAMPE)
            );

        // options.addOption(optionBuilder
        // .withArgName("NUM")
        // .withDescription("Max #diff (int) or missing prob under 0.02 err rate (float) [0.04]")
        // .hasArg()
        // .isRequired(false)
        // .create('n')
        // );
        //
        // options.addOption(optionBuilder
        // .withArgName("INT")
        // .withDescription("Maximum number or fraction of gap opens [1]")
        // .hasArg()
        // .isRequired(false)
        // .create('o')
        // );
        //
        // options.addOption(optionBuilder
        // .withArgName("INT")
        // .withDescription("Maximum number of gap extensions, -1 for disabling long gaps [-1]")
        // .hasArg()
        // .isRequired(false)
        // .create('e')
        // );
        //
        // options.addOption(optionBuilder
        // .withArgName("INT")
        // .withDescription("Do not put an indel within INT bp towards the ends [5]")
        // .hasArg()
        // .isRequired(false)
        // .create('i')
        // );
        //
        // options.addOption(optionBuilder
        // .withArgName("INT")
        // .withDescription("Maximum occurrences for extending a long deletion [10]")
        // .hasArg()
        // .isRequired(false)
        // .create('d')
        // );
        //
        // options.addOption(optionBuilder
        // .withArgName("INT")
        // .withDescription("Seed length [32]")
        // .hasArg()
        // .isRequired(false)
        // .create('l')
        // );
        //
        // options.addOption(optionBuilder
        // .withArgName("INT")
        // .withDescription("Maximum differences in the seed [2]")
        // .hasArg()
        // .isRequired(false)
        // .create('k')
        // );
        //
        // options.addOption(optionBuilder
        // .withArgName("INT")
        // .withDescription("Maximum entries in the queue [2000000]")
        // .hasArg()
        // .isRequired(false)
        // .create('m')
        // );
        //
        // options.addOption(optionBuilder
        // .withArgName("INT")
        // .withDescription("Number of threads per node [1]")
        // .hasArg()
        // .isRequired(false)
        // .create('t')
        // );
        //
        // options.addOption(optionBuilder
        // .withArgName("INT")
        // .withDescription("Mismatch penalty [3]")
        // .hasArg()
        // .isRequired(false)
        // .create('M')
        // );
        // options.addOption(optionBuilder
        // .withArgName("INT")
        // .withDescription("Gap open penalty [11]")
        // .hasArg()
        // .isRequired(false)
        // .create('O')
        // );
        //
        // options.addOption(optionBuilder
        // .withArgName("INT")
        // .withDescription("Gap extension penalty [4]")
        // .hasArg()
        // .isRequired(false)
        // .create('E')
        // );
        //
        // options.addOption(optionBuilder
        // .withArgName("INT")
        // .withDescription("Stop searching when there are >INT equally best hits [30]")
        // .hasArg()
        // .isRequired(false)
        // .create('R')
        // );
        //
        // options.addOption(optionBuilder
        // .withArgName("INT")
        // .withDescription("Quality threshold for read trimming down to 35bp [0]")
        // .hasArg()
        // .isRequired(false)
        // .create('q')
        // );
        //
        // // options.addOption(optionBuilder
        // // .withArgName("FILE")
        // // .withDescription("File to write output to instead of stdout")
        // // .hasArg()
        // // .isRequired(false)
        // // .create('f')
        // // );
        //
        // options.addOption(optionBuilder
        // .withArgName("INT")
        // .withDescription("Length of barcode")
        // .hasArg()
        // .isRequired(false)
        // .create('B')
        // );
        //
        // options.addOption(optionBuilder
        // .withDescription("Input sequences are in the color space")
        // .isRequired(false)
        // .create('c')
        // );
        //
        // options.addOption(optionBuilder
        // .withDescription("Log-scaled gap penalty for long deletions")
        // .isRequired(false)
        // .create('L')
        // );
        //
        // options.addOption(optionBuilder
        // .withDescription("Non-iterative mode: search for all n-difference hits (slooow)")
        // .isRequired(false)
        // .create('N')
        // );
        //
        // options.addOption(optionBuilder
        // .withDescription("The input is in the Illumina 1.3+ FASTQ-like format")
        // .isRequired(false)
        // .create('I')
        // );
        //
        // options.addOption(optionBuilder
        // .withDescription("The input read file is in the BAM format")
        // .isRequired(false)
        // .create('b')
        // );
        //
        // options.addOption(optionBuilder
        // .withDescription("Use single-end reads only (effective with -b)")
        // .isRequired(false)
        // .create('0')
        // );
        //
        // options.addOption(optionBuilder
        // .withDescription("Use the 1st read in a pair (effective with -b)")
        // .isRequired(false)
        // .create('1')
        // );
        //
        // options.addOption(optionBuilder
        // .withDescription("Use the 2nd read in a pair (effective with -b)")
        // .isRequired(false)
        // .create('2')
        // );
        /** @formatter:on */

        return options;
    }

    /**
     * {@inheritDoc}
     * 
     * @see edu.cshl.schatz.jnomics.mapreduce.JnomicsTool#handleOptions(org.apache.commons.cli.CommandLine)
     */
    @Override
    public int handleOptions(CommandLine cmd) throws ParseException, IOException {
        for (String unrecognizedOption : cmd.getArgs()) {
            if (unrecognizedOption.startsWith("-")) {
                throw new ParseException("Unrecognized option " + unrecognizedOption);
            }
        }

        // Loop over parameters we found, and handle them one at a time. This
        // lets us print a warning if we added an option we forgot to handle
        // here.
        for (Option o : cmd.getOptions()) {
            String opt = o.getOpt();

            if (opt.equals(OPT_FASTA_DB)) {
                getConf().set(JnomicsJob.P_REFERENCE_GENOME, o.getValue());
            } else if (opt.equals(OPT_ARGS_BWA)) {
                getConf().set(P_BINARY_ALN_ARGS, o.getValue());
            } else if (opt.equals(OPT_ARGS_SAMPE)) {
                getConf().set(P_BINARY_SAMXE_ARGS, o.getValue());
            } else {
                LOG.warn("Unhandled parameter: " + opt + " " + o.getValue());
            }
        }

        // Temporary: this will soon use the new distributed acche framework
        // (when that's working)

        Path bin = findLocalCommand("bwa"), cache;
        if (bin == null) {
            throw new FileNotFoundException("Could not find bwa binary");
        } else {
            System.out.println("Found bwa binary: "
                    + FileSystem.getLocal(getConf()).makeQualified(bin).toUri().toString());

            cache = distributeIfNew(bin.toString(), "dist/bwa");

            LOG.info("Binary cached: " + bin.toUri() + " --> " + cache.toUri());

            getConf().set(P_BINARY_PATH, cache.toUri().toString());
        }

        // File binaryDir;
        // if (null == (binaryDir = findLocalFile("bwa"))) {
        // throw new FileNotFoundException("Unable to find the bwa binary");
        // } else {
        // getConf().set(P_BINARY_PATH,
        // binaryDir.getParentFile().getCanonicalPath());
        // }

        return STATUS_OK;
    }

    /**
     * {@inheritDoc}
     * 
     * @see edu.cshl.schatz.jnomics.mapreduce.JnomicsTool#run(java.lang.String[])
     */
    @Override
    public int run(String[] args) throws Exception {
        getJob().setReducerClass(DistributedBWAReducerO.class);

        return getJob().waitForCompletion() ? 0 : 1;
    }

    /**
     * @author Matthew A. Titmus
     */
    public static class DistributedBWAReducerO
            extends JnomicsReducerO<Writable, QueryTemplate, Writable, QueryTemplate> {

        private String binaryAlnArgs, binarySamxeArgs;

        private File binaryFile;

        private String binaryPath;

        private final String counterName = this.getClass().getSimpleName();

        private File fastqFileFirst, fastqFileLast, saiFileFirst, saiFileLast;

        private FastqRecordWriter<Writable, QueryTemplate> localFastqOutFirst, localFastqOutLast;

        private File referenceGenomeFile;

        private String referenceGenomeName;

        private int sampeTemplateCounter = 0, sampeReadCounter = 0;

        /**
         * Now that the map() function has created the input FASTQ files, can
         * perform the alignment (bwa aln) and conversion to SAM format (bwa
         * samse [single-ended] or sampe [paired-ended]).
         * <p>
         * TODO Currently, this assumes that we want to align paired-end reads.
         * When possible, add support for samse vs sampe.
         * 
         * @throws IOException {@inheritDoc}
         * @param context {@inheritDoc}
         * @see org.apache.hadoop.mapreduce.Reducer#cleanup(org.apache.hadoop.mapreduce.Reducer.Context)
         */
        @Override
        protected void cleanup(Context context) throws IOException {
            // Spawn the two "bwa aln" processes to perform the initial
            // alignment and generate the required .sai files. This method will
            // throw an IOException if any of these have an exit status != 0.

            runBwaAlnProcesses(context);

            int processExitValue = runBwaSampeProcess(context);

            context.getCounter(counterName, "Reads from sampe").increment(sampeReadCounter);
            context.getCounter(counterName, "Templates from sampe").increment(sampeTemplateCounter);

            if (processExitValue != 0) {
                String msg = String.format(
                    "Process 'bwa sampe' (job_name=%s; job_id=%s) exited with a value of %d.",
                    context.getJobName(), context.getJobID(), processExitValue);

                throw new IOException(msg);
            }
        }

        /**
         * This method is called once for each key.
         */
        @Override
        protected void reduce(Writable key, Iterable<QueryTemplate> iterable, Context context)
                throws IOException, InterruptedException {

            for (QueryTemplate template : iterable) {
                // Write the lines to the local file system in FASTQ format.

                localFastqOutFirst.write(key, template);
                localFastqOutLast.write(key, template);
            }
        }

        /**
         * Spawns instances of {@link BwaAlnThread} for each of the the two
         * FASTQ input files, starts them, and waits for them to complete. Each
         * thread runs an aligner process, generating one .sai each.
         * 
         * @throws IOException
         * @throws InterruptedException
         */
        private void runBwaAlnProcesses(Context context) throws IOException {
            BwaAlnThread[] alignerThreads = new BwaAlnThread[2];

            alignerThreads[0] = new BwaAlnThread(
                context, binaryFile, referenceGenomeFile, fastqFileFirst, saiFileFirst,
                binaryAlnArgs);
            alignerThreads[1] = new BwaAlnThread(
                context, binaryFile, referenceGenomeFile, fastqFileLast, saiFileLast, binaryAlnArgs);

            for (BwaAlnThread thread : alignerThreads) {
                thread.start();
            }

            for (BwaAlnThread thread : alignerThreads) {
                boolean done = false;
                IOException exception = null;

                while (!done) {
                    try {
                        thread.join();
                    } catch (InterruptedException e) {
                        // No-op
                    } finally {
                        done = ((thread.processExitValue != null) || (thread.throwable != null));
                    }
                }

                if ((thread.processExitValue != null) && (thread.processExitValue != 0)) {
                    String msg = String.format(
                        "Process \"%s\" (job_name=%s; job_id=%s) exited with a value of %d.",
                        "bwa aln", context.getJobName(), context.getJobID(),
                        thread.processExitValue);
                    LOG.error(msg);
                } else {
                    String msg = String.format(
                        "Process \"%s\" (job_name=%s; job_id=%s) exited succesfully.", "bwa aln",
                        context.getJobName(), context.getJobID());
                    LOG.info(msg);
                }

                if (thread.throwable != null) {
                    LOG.error(exception);

                    if (thread.throwable instanceof IOException) {
                        exception = (IOException) thread.throwable;
                    } else {
                        exception = new IOException(thread.getClass().getSimpleName() + ": "
                                + thread.throwable.getMessage());

                        exception.setStackTrace(thread.getStackTrace());
                    }
                }

                if (exception != null) {
                    throw exception;
                }
            }
        }

        /**
         * Spawns instances of {@link BwaAlnThread} for each of the the two
         * fastq input files, starts them, and waits for them to complete. Each
         * thread runs an aligner process, generating one .sai each. Stderr
         * output generated by the process is redirected to the
         * {@link System#err}.
         * 
         * @throws IOException thrown by the underlying filesystem or I/O
         *             operations, or if the {@link Process} throws any
         *             exception (particularly an {@link InterruptedException}).
         *             Non-0 status codes DO NOT generate an exception.
         */
        private int runBwaSampeProcess(Context context) throws IOException {
            // Now that the .sai files have been generated, we run sampe/samse
            // to convert the sai files into SAM format. Format: bwa sampe
            // [options] <in.db.fasta> <in1.sai> <in2.sai> <in1.fq>

            final String errorMsg = "'bwa sampe' process failed [exit-status=%s, task-attempt=%s; "
                    + "template=%s; num-reads=%d]";

            final String processCommand = String.format("%s sampe %s %s %s %s %s %s", //
                binaryFile.getCanonicalPath(), //
                binarySamxeArgs, //
                referenceGenomeFile.getCanonicalPath(), //
                saiFileFirst.getCanonicalPath(), //
                saiFileLast.getCanonicalPath(), //
                fastqFileFirst.getCanonicalPath(), //
                fastqFileLast.getCanonicalPath());

            LOG.info("Executing: " + processCommand);

            // BWA samse/sampe generates SAM-formatted output. To efficiently
            // handle the input we use a SAMSequenceReader, which writes reads
            // from the same query template together into a QueryTemplate
            // object, which we in turn push into MapReduce as a single unit.
            // Very convenient.

            QueryTemplate queryTemplate = new QueryTemplate();
            Configuration config = context.getConfiguration();
            int processExitStatus = -1;

            try {
                Process process;
                InputStream stderrStream;
                StreamThread stderrThread;
                SAMLineReader reader;

                process = Runtime.getRuntime().exec(processCommand);

                stderrStream = new BufferedInputStream(process.getErrorStream());
                stderrThread = new StreamThread(stderrStream, System.err);
                stderrThread.start();

                reader = new SAMLineReader(process.getInputStream(), config);

                while (0 != reader.readRecord(queryTemplate)) {
                    sampeTemplateCounter++;
                    sampeReadCounter += queryTemplate.size();

                    context.write(queryTemplate.getTemplateName(), queryTemplate);
                }

                // Wait for the process to end, and grab its return value.
                // InterruptedExceptions are rare, but they can be a pain when
                // they arise, so be insistent.

                processExitStatus = process.waitFor();
            } catch (Exception e) {
                if (e instanceof IOException) {
                    throw (IOException) e;
                } else {
                    String msg = String.format(
                        errorMsg, processExitStatus, context.getTaskAttemptID().toString(),
                        queryTemplate.getTemplateNameString(), queryTemplate.size());

                    throw new IOException(msg, e);
                }
            }

            return processExitStatus;
        }

        /**
         * {@inheritDoc}
         * 
         * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Get configuration properties
            Configuration conf = context.getConfiguration();

            referenceGenomeName = getReq(conf, JnomicsJob.P_REFERENCE_GENOME);
            binaryPath = getReq(conf, P_BINARY_PATH);
            binaryAlnArgs = conf.get(P_BINARY_ALN_ARGS, "");
            binarySamxeArgs = conf.get(P_BINARY_SAMXE_ARGS, "");

            { // TODO Replace this with the new file distribution framework
                final String OUT_DIR = "mapred.output.dir";

                FileSystem fs = FileSystem.get(conf);
                FileSystem fsLocal = FileSystem.getLocal(conf);
                Path sourcePath = new Path("dist/bwa");

                /*Path cachePath = DistributedCache.getLocalCache(sourcePath.toUri(), //
                    conf, //
                    new Path(conf.get(OUT_DIR) + "/cache/"), //
                    fs.getFileStatus(sourcePath), //
                    false, //
                    fs.getFileStatus(sourcePath).getModificationTime(), //
                    new Path(conf.get(OUT_DIR)), //
                    true);

                cachePath = fsLocal.makeQualified(cachePath);

                LOG.info(String.format(
                    "Local cache: %s (Exists=%s)%n", cachePath.toString(),
                    fsLocal.exists(cachePath)));

                binaryPath = new File(cachePath.toUri()).getCanonicalPath();*/
            }

            // These are REQUIRED to exist. If they don't, throw a
            // FileNotFoundException
            checkExists(binaryFile = new File(binaryPath));
            checkExists(referenceGenomeFile = new File(referenceGenomeName));

            // Create temporary FASTA files. We also register them for automatic
            // deletion once the JVM exits, since we don't want to fill up the
            // node's local disks with these (possibly large) files!

            String taskAttemptId = context.getTaskAttemptID().toString();

            fastqFileFirst = File.createTempFile(taskAttemptId, ".1.fq");
            fastqFileLast = File.createTempFile(taskAttemptId, ".2.fq");
            // fastqFileFirst = new File(taskAttemptId + ".2.fq");
            // fastqFileLast = new File(taskAttemptId + ".2.fq");
            fastqFileFirst.deleteOnExit();
            fastqFileLast.deleteOnExit();

            saiFileFirst = new File(fastqFileFirst.getCanonicalPath() + ".sai");
            saiFileLast = new File(fastqFileLast.getCanonicalPath() + ".sai");
            saiFileFirst.deleteOnExit();
            saiFileLast.deleteOnExit();

            localFastqOutFirst = new FastqRecordWriter<Writable, QueryTemplate>(
                new DataOutputStream(new FileOutputStream(fastqFileFirst)), conf,
                FastqRecordWriter.MEMBER_FIRST);

            localFastqOutLast = new FastqRecordWriter<Writable, QueryTemplate>(
                new DataOutputStream(new FileOutputStream(fastqFileLast)), conf,
                FastqRecordWriter.MEMBER_LAST);
        }

        /**
         * Executes a 'bwa aln' process, which finds the suffix array (SA)
         * coordinates of good alignments for each individual read, which will
         * subsequently be converted to chromosomal coordinates and (for
         * 'sampe') reads pairs.
         * <p>
         * Parameters accepted by the 'bwa aln' binary:
         * 
         * <pre>
         * Usage:   bwa aln [options] <in.db.fasta> <in.query.fq> > <out.sai> 
         * Example: bwa aln database.fasta short_read.fastq > aln_sa.sai
         * 
         * Options: -n NUM    max #diff (int) or missing prob under 0.02 err rate (float) [0.04]
         *          -o INT    maximum number or fraction of gap opens [1]
         *          -e INT    maximum number of gap extensions, -1 for disabling long gaps [-1]
         *          -i INT    do not put an indel within INT bp towards the ends [5]
         *          -d INT    maximum occurrences for extending a long deletion [10]
         *          -l INT    seed length [32]
         *          -k INT    maximum differences in the seed [2]
         *          -m INT    maximum entries in the queue [2000000]
         *          -t INT    number of threads [1]
         *          -M INT    mismatch penalty [3]
         *          -O INT    gap open penalty [11]
         *          -E INT    gap extension penalty [4]
         *          -R INT    stop searching when there are >INT equally best hits [30]
         *          -q INT    quality threshold for read trimming down to 35bp [0]
         *          -f FILE   file to write output to instead of stdout
         *          -B INT    length of barcode
         *          -c        input sequences are in the color space
         *          -L        log-scaled gap penalty for long deletions
         *          -N        non-iterative mode: search for all n-difference hits (slooow)
         *          -I        the input is in the Illumina 1.3+ FASTQ-like format
         *          -b        the input read file is in the BAM format
         *          -0        use single-end reads only (effective with -b)
         *          -1        use the 1st read in a pair (effective with -b)
         *          -2        use the 2nd read in a pair (effective with -b)
         * </pre>
         * 
         * </p>
         * 
         * @author Matthew Titmus
         */
        private class BwaAlnThread extends Thread {
            /**
             * The exit value of the process as an int/Integer. Initial value is
             * <code>null</code>.
             */
            Integer processExitValue = null;

            /**
             * If an exception is thrown in the run(), it gets placed here.
             */
            Throwable throwable = null;

            /**
             * The command line entry to execute.
             */
            private String command;

            private Process process;

            /**
             * This reader captures the contents of the processes' standard
             * error stream and writes it to the process log.
             */
            private InputStream processErr;

            /**
             * The "bwa aln" processes' output (input relative to here).
             * Typically this would be redirected directly to a .sai file on the
             * command line, but we need to take a more elaborate approach.
             */
            private InputStream processIn;

            /**
             * This stream points to the file (.sai) to which the output bytes
             * from the process are written.
             */
            private OutputStream saiFileOutputStream;

            public BwaAlnThread(Context context, File binaryFile, File referenceGenomeFasta,
                File inputFastq, File saiFile, String options) throws IOException {

                setName("bwa aln " + binaryFile.getAbsolutePath());

                // If the number of processors isn't explicitly specified, we
                // set it to be equal to half of 1 plus the number of available
                // processors.
                if (!options.contains("-t ")) {
                    int nProcs = (1 + Runtime.getRuntime().availableProcessors()) / 2;

                    LOG.info("Number of processors not specified for 'bwa aln'; "
                            + "defaulting to " + nProcs + " each (-t " + nProcs + ").");

                    options += (" -t " + nProcs);
                }

                command = String.format(
                    "%s aln %s %s %s", binaryFile.getCanonicalPath(), options,
                    referenceGenomeFasta.getCanonicalPath(), inputFastq.getCanonicalPath());

                saiFileOutputStream = new BufferedOutputStream(new FileOutputStream(saiFile));
            }

            /*
             * @see java.lang.Thread#run()
             */
            @Override
            public void run() {
                LOG.info("Executing process: " + command);

                try {
                    StreamThread stderrThread, stdoutThread;

                    process = Runtime.getRuntime().exec(command);
                    processIn = new BufferedInputStream(process.getInputStream());
                    processErr = new BufferedInputStream(process.getErrorStream());

                    stdoutThread = new StreamThread(processIn, saiFileOutputStream);
                    stdoutThread.start();

                    stderrThread = new StreamThread(processErr, System.err);
                    stderrThread.start();

                    process.waitFor();
                } catch (Throwable t) {
                    throwable = t;
                } finally {
                    if (process != null) {
                        processExitValue = process.exitValue();
                    }

                    try {
                        saiFileOutputStream.flush();
                        saiFileOutputStream.close();
                    } catch (Exception e) {/* No-op */}
                }
            }
        }
    }
}
