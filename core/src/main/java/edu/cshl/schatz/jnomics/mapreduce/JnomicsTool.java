/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.mapreduce;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.cshl.schatz.jnomics.cli.OptionBuilder;
import edu.cshl.schatz.jnomics.cli.OptionWeightComparater;
import edu.cshl.schatz.jnomics.cli.TolerantParser;
import edu.cshl.schatz.jnomics.io.JnomicsOutputFormat;

/**
 * <p>
 * <i><b>TODO</b> - This is an important class: it should be commented!</i>
 * </p>
 * <p>
 * NOTE: Status codes <code><= 0</code> are used to indicate non-error
 * conditions in which some special action was taken. Methods that (and should
 * themselves return <code>0</code>), For example: if no parameters are received
 * from the command line, the {@link JnomicsTool} output the help menu to stdout
 * and returns a <code>-1</code> status.
 * <p>
 * 
 * @author Matthew Titmus
 */
public abstract class JnomicsTool extends Configured implements Tool {
    public static final Pattern SCHEMA_EXISTS_PATTERN = Pattern.compile(
        "^[a-z0-9:]+:/.+$", Pattern.CASE_INSENSITIVE);

    /**
     * Standard Jnomics status code to indicate a user error in which the
     * parameters contained an unexpected value or condition.
     */
    public static final int STATUS_ERROR_BAD_ARGS = 2;

    /**
     * Standard Jnomics status code for a non-specific error. It's good practice
     * to only use this if none of the standard codes apply.
     */
    public static final int STATUS_ERROR_GENERAL = 1;

    /**
     * Standard Jnomics status code for an internal error that is not the fault
     * of the user. Generally, the method returning value should try to output
     * some useful information (i.e., a stack trace) as well.
     */
    public static final int STATUS_ERROR_INTERNAL = 5;

    /**
     * The universal status code indicating success.
     */
    public static final int STATUS_OK = 0;

    /**
     * Standard Jnomics status code used to indicates no parameters were
     * received from the command line, and that the tool responded by outputting
     * the help menu stdout (and took no other actions).
     */
    public static final int STATUS_OK_NO_PARAMS = -1;

    static final Log LOG = LogFactory.getLog(JnomicsTool.class);

    private HelpFormatter helpFormatter;

    private String helpHeading = null;

    private String helpUsage = "<undefined; please specify a usage via JnomicsTool.setUsage(String)>";

    private JnomicsJob job;

    /**
     * Builds a new JnomicsTool using a a default {@link Configuration}, and
     * connecting to the default {@link JobTracker}.
     */
    public JnomicsTool() {}

    /**
     * Builds a new JnomicsTool using a a default {@link Configuration}, and
     * connecting to the default {@link JobTracker}.
     * 
     * @param conf
     */
    public JnomicsTool(Configuration conf) {
        super(conf);
    }

    /**
     * @throws IOException
     */
    public static FileSystem resolveFS(Path path, Configuration conf) throws IOException {
        // If a prefix isn't specified, we check to see if the path
        // exists in the local file system. If it doesn't, we assume resolve
        // according to the default file system.

        if (!SCHEMA_EXISTS_PATTERN.matcher(path.toString()).matches()) {
            FileSystem fsLocal, fsDefault;
            boolean existsLocal, existsDefault;

            fsLocal = FileSystem.getLocal(conf);
            fsDefault = FileSystem.get(conf);

            existsLocal = fsLocal.exists(path);
            existsDefault = fsDefault.exists(path);

            if (existsDefault) {
                // The path exists in the default file system; stop here.

                return fsDefault;
            } else if (existsLocal) {
                // The path exists locally, but not in the default FS.

                return fsLocal;
            } else {
                // The path doesn't exist in EITHER file system; check for
                // the path's PARENT directories. The first one we find we
                // consider the winner, and resolve the original path
                // accordingly.

                Path parent = path.getParent();

                return (parent == null ? fsDefault : resolveFS(parent, conf));
            }
        } else {
            // We found a prefix; return accordingly.

            return FileSystem.get(path.toUri(), conf);
        }
    }

    /**
     * @throws IOException
     */
    public static Path resolvePath(String pathString, Configuration conf) throws IOException {
        Path path = new Path(pathString);

        FileSystem fs = resolveFS(path, conf);

        return fs.makeQualified(path);
    }

    /**
     * Runs the given <code>Tool</code> by {@link Tool#run(String[])}, after
     * parsing with the given generic arguments. Uses the given
     * <code>Configuration</code>, or builds one if null. Sets the
     * <code>Tool</code>'s configuration with the possibly modified version of
     * the <code>conf</code>.
     * 
     * @param tool <code>Tool</code> to run.
     * @param args command-line arguments to the tool.
     * @return exit code of the {@link Tool#run(String[])} method.
     */
    public static int run(Tool tool, String[] args) throws Exception {
        if (tool instanceof JnomicsTool) {
            JnomicsTool jtool = (JnomicsTool) tool;
            int status;

            if (STATUS_OK < (status = jtool.handleParameters(args))) {
                return status;
            } else if (STATUS_OK > status) {
                // Negative is a non-error exit code; return 0.
                return STATUS_OK;
            }
        }

        try {
            return tool.run(args);
        } catch (Exception e) {
            if (tool.getConf().getBoolean(JnomicsJob.P_VERBOSE, false)) {
                // Flush the standard output stream first, then print the stack.
                System.out.flush();
                e.printStackTrace();
            } else {
                // Non-verbose mode. Just print the exception class and message.
                System.err.printf("%s: %s%n", e.getClass().getName(), e.getMessage());
            }

            return STATUS_ERROR_GENERAL;
        }
    }

    /*
     * @see org.apache.hadoop.conf.Configured#getConf()
     */
    @Override
    public Configuration getConf() {
        if (null == super.getConf()) {
            try {
                setJob(new JnomicsJob());
            } catch (IOException e) {
                LOG.warn("IOException trying to create new JnomicsJob instance", e);
                setConf(new Configuration());
            }
        }

        return super.getConf();
    }

    /**
     * Gets the help output formatter. By default, this is a standard
     * {@link HelpFormatter} instance using an {@link OptionWeightComparater} to
     * sort the options.
     */
    public HelpFormatter getHelpFormatter() {
        if (helpFormatter == null) {
            helpFormatter = new HelpFormatter();
            helpFormatter.setOptionComparator(new OptionWeightComparater());
        }

        return helpFormatter;
    }

    /**
     * @return The helpHeading
     */
    public String getHelpHeading() {
        return helpHeading;
    }

    /**
     * @return The helpUsage
     */
    public String getHelpUsage() {
        return helpUsage;
    }

    /**
     * Provides the this tool's {@link Job} instance. If a {@link Job} does not
     * yet exist it will create a new {@link JnomicsJob} instance for this tool.
     * 
     * @return The {@link Job} instance associated with this tool.
     * @throws IOException
     */
    public JnomicsJob getJob() throws IOException {
        if (job == null) {
            setJob(new JnomicsJob(getConf()));
        }

        return job;
    }

    /**
     * This method returns an {@link Options} instance containing this Options
     * supported by this tool. The default implementation of this method returns
     * an empty {@link Options} instance.
     * 
     * @return An {@link Options} instance with zero or more options added.
     */
    public Options getOptions() {
        return new Options();
    }

    /**
     * Override this method to handle your own options, but do not call directly
     * since it is called by Called by {@link #handleParameters(String[])}.
     * 
     * @param cmd The {@link CommandLine} instance containing the parsed command
     *            line parameters.
     * @return An integer status code; any value other than STATUS_OK (0) will
     *         cause the process to exit.
     * @throws ParseException
     * @throws IOException
     */
    public int handleOptions(CommandLine cmd) throws ParseException, IOException {
        return STATUS_OK;
    }

    /**
     * Outputs the "help" menu to the standard output stream.
     */
    public void printHelp() {
        printHelp(System.out);
    }

    /**
     * If any options are excluded, they're kept here so the correct help menu
     * can be build if needed.
     */
    protected String[] excludedHadoopOptions = new String[] {},
            excludedJnomicsOptions = new String[] {};

    /**
     * Outputs the "help" menu to the specified {@link PrintStream}.
     */
    public void printHelp(PrintStream outTo) {
        PrintWriter pw = new PrintWriter(outTo);

        Options options = buildDefaultHadoopOptions(
            buildDefaultJnomicsOptions(getOptions(), excludedJnomicsOptions), excludedHadoopOptions);

        getHelpFormatter().printHelp(
            pw, 80, helpUsage, helpHeading, options, HelpFormatter.DEFAULT_LEFT_PAD,
            HelpFormatter.DEFAULT_DESC_PAD, null, false);

        pw.flush();
    }

    /*
     * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
     */
    public int run(String[] args) throws Exception {
        return STATUS_OK;
    }

    /*
     * @see org.apache.hadoop.conf.Configured#setConf(org.apache.hadoop.conf.
     * Configuration)
     */
    @Override
    public void setConf(Configuration conf) {
        super.setConf(conf);
    }

    /**
     * Sets the help output formatter. By default, this is a standard
     * {@link HelpFormatter} instance using an {@link OptionWeightComparater} to
     * sort the options.
     * 
     * @param formatter The helpFormatter to set. If <code>null</code>, this is
     *            reset to the default value described above.
     */
    public void setHelpFormatter(HelpFormatter formatter) {
        this.helpFormatter = formatter;
    }

    /**
     * Sets the header to use on the help output text.
     * 
     * @param helpHeading The helpHeading to set
     */
    public void setHelpHeading(String helpHeading) {
        this.helpHeading = helpHeading;
    }

    /**
     * Sets the "usage" text for the help output text.
     */
    public void setHelpUsage(String helpUsage) {
        this.helpUsage = helpUsage;
    }

    /**
     * Builds and returns an {@link Options} object containing the default
     * Hadoop command line options.
     * 
     * @param options {@link Options} instance to add the default options to.
     *            May be <code>null</code>.
     * @param exclude Zero or more commands to exclude from the options list.
     */
    protected Options buildDefaultHadoopOptions(Options options, String... exclude) {
        OptionBuilder optionBuilder = new OptionBuilder();

        if (null == options) {
            options = new Options();
        }

        Set<String> excludeSet = new HashSet<String>();
        this.excludedHadoopOptions = exclude;

        for (String ex : exclude) {
            excludeSet.add(ex);
        }

        /** @formatter:off */
        if (!excludeSet.contains("fs")) {
            options.addOption(optionBuilder
                .withArgName("local|namenode:port")
                .hasArg()
                .withDescription("Specify a namenode")
                .withWeight(-10)
                .create("fs"));
        }

        if (!excludeSet.contains("jt")) {
            options.addOption(optionBuilder
                .withArgName("local|jobtracker:port")
                .hasArg()
                .withDescription("Specify a job tracker")
                .withWeight(-10)
                .create("jt"));
        }

        if (!excludeSet.contains("conf")) {
            options.addOption(optionBuilder
                .withArgName("configuration file")
                .hasArg()
                .withDescription("Specify an application configuration file")
                .withWeight(-10)
                .create("conf"));
        }

        if (!excludeSet.contains("D")) {
            options.addOption(optionBuilder
                .withArgName("property=value")
                .hasArg()
                .withDescription("Use value for given property")
                .withWeight(-10)
                .create('D'));
        }

        if (!excludeSet.contains("libjars")) {
            options.addOption(optionBuilder
                .withArgName("paths")
                .hasArg()
                .withDescription("Comma separated jar files to include in the classpath.")
                .withWeight(-10)
                .create("libjars"));
        }

        if (!excludeSet.contains("files")) {
            options.addOption(optionBuilder
                .withArgName("paths")
                .hasArg()
                .withDescription("Comma separated files to be copied to the " +
                        "map reduce cluster")
                .withWeight(-10)
                .create("files"));
        }

        if (!excludeSet.contains("archives")) {
            options.addOption(optionBuilder
                .withArgName("paths")
                .hasArg()
                .withDescription("Comma separated archives to be unarchived" +
                        " on the compute machines.")
                .withWeight(-10)
                .create("archives"));
        }
        /** @formatter:on */

        return options;
    }

    /**
     * Builds and returns a new {@link Options} object containing the default
     * Hadoop command line options.
     * 
     * @param exclude Zero or more commands to exclude from the options list.
     */
    protected Options buildDefaultHadoopOptions(String... exclude) {
        return buildDefaultHadoopOptions(null, exclude);
    }

    /**
     * Builds and returns an {@link Options} object containing the default
     * Jnomics command line options.
     * 
     * @param options {@link Options} instance to add the default options to.
     *            May be <code>null</code>.
     * @param exclude Zero or more commands to exclude from the options list.
     */
    protected Options buildDefaultJnomicsOptions(Options options, String... exclude) {
        OptionBuilder optionBuilder = new OptionBuilder();

        if (null == options) {
            options = new Options();
        }

        Set<String> excludeSet = new HashSet<String>();
        this.excludedJnomicsOptions = exclude;

        for (String ex : exclude) {
            excludeSet.add(ex);
        }

        /** @formatter:off */
        if (!excludeSet.contains("fout") && !excludeSet.contains("out-format")) { 
            options.addOption(optionBuilder
                .withLongOpt("out-format")
                .withArgName("fmt")
                .withDescription("File output format. Valid formats are: "
                        + ReadFileFormat.getAvailableFormatsString())
                .withWeight(-5)
                .hasArg()
                .isRequired(false)
                .create("fout"));
        }

        if (!excludeSet.contains("in")) {
            options.addOption(optionBuilder
                .withArgName("file")
                .withDescription("Input file(s). Required.")
                .withWeight(-5)
                .hasArgs()
                .isRequired(true)
                .create("in"));
        }

        if (!excludeSet.contains("out")) {
            options.addOption(optionBuilder
                .withArgName("dir")
                .withDescription("Output path. Required.")
                .withWeight(-5)
                .hasArg()
                .isRequired(true)
                .create("out"));
        }

        if (!excludeSet.contains("v") && !excludeSet.contains("verbose")) {
            options.addOption(optionBuilder
                .withLongOpt("verbose")
                .withDescription("Generates verbose Hadoop output.")
                .withWeight(-5)
                .isRequired(false)
                .create("v"));
        }

        if (!excludeSet.contains("?") && !excludeSet.contains("help")) {
            options.addOption(optionBuilder
                .withLongOpt("help")
                .withDescription("Outputs this list and exits.")
                .withWeight(-20)
                .create("?"));
        }
        /** @formatter:on */

        return options;
    }

    /**
     * Builds and returns an {@link Options} object containing the default
     * Jnomics command line options.
     * 
     * @param exclude Zero or more commands to exclude from the options list.
     */
    protected Options buildDefaultJnomicsOptions(String... exclude) {
        return buildDefaultJnomicsOptions(new Options(), exclude);
    }

    /**
     * This method handles the generic parameters that are specific to the
     * Hadoop framework, as follows:
     * 
     * <pre>
     * -archives &lt;paths&gt;             Comma separated archives to be unarchived on the
     *                               compute machines.
     * -conf &lt;configuration file&gt;    Specify an application configuration file
     * -D &lt;property=value&gt;           Use value for given property
     * -files &lt;paths&gt;                Comma separated files to be copied to the map
     *                               reduce cluster
     * -fs &lt;local|namenode:port&gt;     Specify a namenode
     * -jt &lt;local|jobtracker:port&gt;   Specify a job tracker
     * -libjars &lt;paths&gt;              Comma separated jar files to include in the
     *                               classpath.
     * </pre>
     * 
     * @param line A {@link CommandLine} instance, probably generated by the
     *            {@link #parseParameters(String[], Options)} method.
     */
    protected int handleDefaultHadoopOptions(CommandLine line) {
        Configuration conf = getConf();

        if (line.hasOption("fs")) {
            FileSystem.setDefaultUri(conf, line.getOptionValue("fs"));
        }

        if (line.hasOption("jt")) {
            conf.set("mapred.job.tracker", line.getOptionValue("jt"));
        }

        if (line.hasOption("conf")) {
            String[] values = line.getOptionValues("conf");
            for (String value : values) {
                conf.addResource(new Path(value));
            }
        }

        try {
            if (line.hasOption("libjars")) {
                conf.set("tmpjars", validateFiles(line.getOptionValue("libjars"), conf));
                // setting libjars in client classpath
                URL[] libjars = GenericOptionsParser.getLibJars(conf);
                if ((libjars != null) && (libjars.length > 0)) {
                    conf.setClassLoader(new URLClassLoader(libjars, conf.getClassLoader()));
                    Thread.currentThread().setContextClassLoader(
                        new URLClassLoader(libjars, Thread.currentThread().getContextClassLoader()));
                }
            }
            if (line.hasOption("files")) {
                conf.set("tmpfiles", validateFiles(line.getOptionValue("files"), conf));
            }
            if (line.hasOption("archives")) {
                conf.set("tmparchives", validateFiles(line.getOptionValue("archives"), conf));
            }
        } catch (IOException ioe) {
            System.err.println(StringUtils.stringifyException(ioe));
        }

        if (line.hasOption('D')) {
            String[] property = line.getOptionValues('D');
            for (String prop : property) {
                String[] keyval = prop.split("=", 2);
                if (keyval.length == 2) {
                    conf.set(keyval[0], keyval[1]);
                }
            }
        }

        conf.setBoolean("mapred.used.genericoptionsparser", true);

        return STATUS_OK;
    }

    /**
     * Called by {@link #handleParameters(String[])}.
     * 
     * @param cmd The {@link CommandLine} instance containing the parsed command
     *            line parameters.
     * @return An integer status code; any value other than {@link #STATUS_OK}
     *         (0) will cause the process to exit.
     * @throws ParseException
     * @throws IOException
     */
    protected int handleDefaultJnomicsOptions(CommandLine cmd) throws ParseException, IOException {
        if (cmd.hasOption('?')) {
            printHelp();
        } else {
            if (cmd.hasOption("v")) {
                getConf().setBoolean(JnomicsJob.P_VERBOSE, true);
            }

            getJob().setSequencingReadOutputFormat(cmd.getOptionValue("fout", "sam"));

            if (cmd.hasOption("in")) {
                for (String inputPath : cmd.getOptionValues("in")) {
                    FileInputFormat.addInputPath(getJob(), resolvePath(inputPath, getConf()));
                }
            }

            if (cmd.hasOption("out")) {
                JnomicsOutputFormat.setOutputPath(getJob(), new Path(cmd.getOptionValue("out")));
            }
        }

        return STATUS_OK;
    }

    /**
     * Called by {@link #run(Tool, String[])} to parse and handle all command
     * line options.
     * <p>
     * If <code>args.length == 0</code>, then {@link #printHelp()} is called and
     * the method returns a value of <code>STATUS_OK</code>. Otherwise,
     * <tt>args</tt> is parsed by a {@link TolerantParser}, and the resulting
     * {@link CommandLine} passed, in order, to
     * {@link #handleDefaultHadoopOptions(CommandLine)},
     * {@link #handleDefaultJnomicsOptions(CommandLine)}, and
     * {@link #handleOptions(CommandLine)}. Each "handle" method only receives
     * the parameters that were not handled by the method before it.
     * 
     * @see {@link CommandLineParser#parse(Options, String[])}
     * @param args
     * @return A response code: {@link #STATUS_OK} (<tt>0</tt> if there is no
     *         error; a value <code>&gt;0</code> otherwise.
     * @throws ParseException
     */
    protected int handleParameters(String args[]) throws Exception {
        CommandLineParser parser = new TolerantParser();
        CommandLine clHadoop, clJnomics, clCustom;
        String tier = "";

        // If we get no incoming options, or if we see the "help" command, print
        // the help text and exit.

        if (args.length == 0) {
            printHelp(System.out);
            return STATUS_OK_NO_PARAMS;
        }

        // We check against status codes in addition to catching exceptions.
        // This allows a downstream implementor to handle errors themselves and
        // then generate specific codes for specific events.
        try {
            int response;

            tier = "Error parsing default Hadoop parameter: ";
            clHadoop = parser.parse(buildDefaultHadoopOptions(), args, false);
            if (STATUS_OK < (response = handleDefaultHadoopOptions(clHadoop))) {
                return response;
            }

            tier = "Error parsing default Jnomics parameter: ";
            args = clHadoop.getArgs();
            clJnomics = parser.parse(buildDefaultJnomicsOptions(), args, false);
            if (STATUS_OK < (response = handleDefaultJnomicsOptions(clJnomics))) {
                return response;
            }

            tier = "Error parsing command parameter: ";
            args = clJnomics.getArgs();
            clCustom = parser.parse(getOptions(), args, false);
            if (STATUS_OK < (response = handleOptions(clCustom))) {
                return response;
            }
        } catch (ParseException e) {
            System.err.println(tier + e.getLocalizedMessage());

            return STATUS_ERROR_BAD_ARGS;
        }

        return STATUS_OK;
    }

    /**
     * Parses a String array according to a set of {@link Option}s and generates
     * a {@link CommandLine} object.
     */
    protected CommandLine parseParameters(String[] args, Options options) throws ParseException {
        CommandLine commandLine = null;
        CommandLineParser parser = new GnuParser();

        commandLine = parser.parse(options, args, true);

        if (0 != commandLine.getArgs().length) {
            String errorMessage = "Unrecognized option(s): ";

            for (String unrecognizedOption : commandLine.getArgs()) {
                if (unrecognizedOption.startsWith("-")) {
                    errorMessage += unrecognizedOption;
                }
            }

            throw new ParseException(errorMessage);
        }

        return commandLine;
    }

    /**
     * Sets the job to be used by this tool. It it called by the
     * {@link ToolRunner}, and generally shouldn't be called manually.
     */
    protected void setJob(JnomicsJob job) {
        this.job = job;
        setConf(job.getConfiguration());
    }

    /**
     * <p>
     * Borrowed directly from the Hadoop 20.2 code base. Thanks, guys.
     * <p>
     * Takes input as a comma separated list of files and verifies whether they
     * exist. It defaults to file:/// if the files specified do not have a
     * scheme. For example, the string "/home/user/file1,/home/user/file2" would
     * return "file:///home/user/file1,file:///home/user/file2".
     * 
     * @author Hadoop 20.2
     * @param files A comma-separated list of files.
     * @return Another comma-separated string, containing the input paths'
     *         converted to valid URIs.
     */
    private String validateFiles(String files, Configuration conf) throws IOException {
        if (files == null) {
            return null;
        }

        String[] fileArr = files.split(",");
        String[] finalArr = new String[fileArr.length];

        for (int i = 0; i < fileArr.length; i++) {
            String tmp = fileArr[i];
            String finalPath;
            Path path = new Path(tmp);
            URI pathURI = path.toUri();
            FileSystem localFs = FileSystem.getLocal(conf);

            if (pathURI.getScheme() == null) {
                // default to the local file system
                // check if the file exists or not first
                if (!localFs.exists(path)) {
                    throw new FileNotFoundException("File " + tmp + " does not exist.");
                }

                finalPath = path.makeQualified(localFs).toString();
            } else {
                // check if the file exists in this file system
                // we need to recreate this filesystem object to copy
                // these files to the file system jobtracker is running
                // on.
                FileSystem fs = path.getFileSystem(conf);

                if (!fs.exists(path)) {
                    throw new FileNotFoundException("File " + tmp + " does not exist.");
                }

                finalPath = path.makeQualified(fs).toString();

                try {
                    fs.close();
                } catch (IOException e) {}
            }
            finalArr[i] = finalPath;
        }

        return StringUtils.arrayToString(finalArr);
    }
}
