/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.tools;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMFileWriter;
import net.sf.samtools.SAMFileWriterFactory;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMRecordIterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.cshl.schatz.jnomics.cli.OptionBuilder;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsTool;

/**
 * TODO Add support for compression formats (-f zip|gzip|zlib)
 * 
 * @author Matthew Titmus
 */
public class ConvertCopy extends JnomicsTool {
    /**
     * 
     */
    public ConvertCopy() {
        setHelpHeading(HELP_HEADER);
        setHelpUsage(HELP_USAGE);
    }

    public static final String CMD_DESCRIPTION = "Perform BAM->SAM "
            + "conversion from a URI and stream results to another URI "
            + "(file://, hdfs://, etc).";

    public static final String CMD_NAME = "convertcopy";

    public static final String DEFAULT_HDFS = "?";

    // private static final String REGEX =
    // "hdfs://[a-z0-9_\\.\\-\\~]+:[0-9]{1,5}([a-z0-9_\\.\\-\\~/])*/?$";

    private static final String HELP_FOOTER = "\n\nThe -i and -o arguments "
            + "expects a target URI, which may be absolute or relative. Legal "
            + "URI schemes are hdfs:// and file://; if the scheme is excluded, "
            + "the argument is interpreted relative to the default filesystem.\n"
            + "Typically, a URI with an hdfs schema is expected to include the "
            + "host namenode URL (as in hdfs://localhost:9000/some/path), but "
            + "for convenience, if the host URL is replaced with '?', then it "
            + "is interpreted to mean the default HDFS as defined in the "
            + "Hadoop configuration.";

    private static final String HELP_HEADER = "\n\nCopies a file from and to "
            + "specified URIs, even between two distinct filesystems. If "
            + "the subject file is a BAM file, it will be converted to a SAM "
            + "file (unless the -x flag is present).\n|\nAccepted parameters:\n";

    private static final String HELP_USAGE = CMD_NAME
            + " [-?] [-i <stdin|uri>] [-o <stdout|uri>] [-p] [-v] [-x]";

    private static final char OPTION_IN = 'i';
    private static final String OPTION_IN_LONG = "in";

    private static final char OPTION_NOCONVERT = 'x';
    private static final String OPTION_NOCONVERT_LONG = "no-convert";

    private static final char OPTION_OUT = 'o';
    private static final String OPTION_OUT_LONG = "out";

    private static final char OPTION_PATHS = 'p';
    private static final String OPTION_PATHS_LONG = "paths";

    private static final char OPTION_VERBOSE = 'v';
    private static final String OPTION_VERBOSE_LONG = "verbose";

    private InputStream in = null;

    private boolean optionNoConvert = false;

    private boolean optionPathsOnly = false;

    private boolean optionVerbose = false;

    private OutputStream out = null;

    /**
     * Searches file systems for a file and returns its fully-qualified URI if
     * it exists; <code>null</code> otherwise. If no URI schema (
     * <code>hdfs://</code>, <code>file://</code> is included, this method will
     * check first in the local file system, and then in the default (assumedly
     * hdfs) file system.
     * 
     * @param uri A URI indicating the file location.
     * @return The qualified URL if the file exists; <code>null</code>
     *         otherwise.
     */
    public static String exists(String uri, Configuration c) throws IOException, URISyntaxException {

        String newURI;

        // If a schema exists
        if (!uri.matches("[a-zA-Z0-9]*://.*$")) {
            return interpretURI(uri, c);

            // Check local and default file-systems, in that order.
        } else if (FileSystem.get(new URI(newURI = interpretURI("file://" + uri, c)), c).exists(
            new Path(newURI))
                || FileSystem.get(new URI(newURI = interpretURI(uri, c)), c).exists(
                    new Path(newURI))) {

            return newURI;
        } else {
            return null;
        }
    }

    /**
     * <p>
     * Interprets an URI or path (absolute or relative) into a complete and
     * fully-qualified URI. If the URI scheme is omitted, the argument is
     * interpreted as a reference to the default filesystem as defined by the
     * <code>fs.default.name</code> property.
     * <p>
     * Typically, a URI with an hdfs schema is expected to include the host
     * namenode URL (as in hdfs://localhost:9000/some/path), but for
     * convenience, if the host URL is replaced with '?', then it is interpreted
     * to mean the default HDFS as defined in the Hadoop configuration.
     * </p>
     * <table>
     * <tr>
     * <th>Example argument</th>
     * <th><code>fs.default.name</code></th>
     * <th>Example return value</th>
     * </tr>
     * <tr>
     * <td><code>relative.foo</code></td>
     * <td><code>file:/</code></td>
     * <td><code>file:/absolute/path/relative.foo</code></td>
     * </tr>
     * <tr>
     * <td><code>relative.foo</code></td>
     * <td><code>hdfs://host:1234/</code></td>
     * <td><code>hdfs://host:1234/user/alice/relative.foo</code></td>
     * </tr>
     * <tr>
     * <td><code>/blah/absolute.foo</code></td>
     * <td><code>file:/</code></td>
     * <td><code>file:/blah/absolute.foo</code></td>
     * </tr>
     * <tr>
     * <td><code>/blah/absolute.foo</code></td>
     * <td><code>hdfs://host:1234/</code></td>
     * <td><code>hdfs://host:1234/blah/absolute.foo</code></td>
     * </tr>
     * <tr>
     * <td><code>hdfs://?/path/foo.bar</code></td>
     * <td><code>hdfs://host:1234/</code></td>
     * <td><code>file:/path/foo.bar</code></td>
     * </tr>
     * <tr>
     * <td><code>hdfs://?/path/foo.bar</code></td>
     * <td><code>hdfs://host:1234/path/foo.bar</code></td>
     * <td><code>file:/path/foo.bar</code></td>
     * </tr>
     * </table>
     * 
     * @deprecated
     * @param uri
     * @param conf
     * @throws IOException
     * @throws URISyntaxException
     */
    public static String interpretURI(String uri, Configuration conf)
            throws IOException, URISyntaxException {

        String defaultHdfsSchema = "hdfs://" + DEFAULT_HDFS + "/";
        String newURI = uri;

        if (uri.toLowerCase().startsWith(defaultHdfsSchema)) {
            if (null == (newURI = conf.get("fs.default.name"))) {
                System.err.println("WARNING: fs.default.name is null; using file://");
                newURI = "file://";
            }

            newURI += uri.substring(defaultHdfsSchema.length());
        }

        FileSystem fs = FileSystem.get(new URI(newURI), conf);

        return new Path(newURI).makeQualified(fs).toString();
    }

    public static void main(String[] args) throws Exception {
        JnomicsTool.run(new ConvertCopy(), args);
    }

    /**
     * Performs a copy and BAM-->SAM conversion from <code>in</code> to
     * <code>out</code>, until the end of the stream is reached. If the source
     * file is already in SAM format, then no conversion is attempted.
     * 
     * @param in A stream of SAM/BAM-formatted text
     * @param out The target output stream.
     * @throws IOException
     * @return The number of records converted.
     */
    public int doBAMConvertCopy(InputStream in, OutputStream out) throws IOException {
        SAMFileReader samIn = new SAMFileReader(in);

        SAMRecordIterator iterator = samIn.iterator();
        SAMRecord record = iterator.next();
        SAMFileHeader header = record.getHeader();
        SAMFileWriter writer = new SAMFileWriterFactory().makeSAMWriter(header, true, out);
        int count = 0;

        writer.addAlignment(record);

        while (iterator.hasNext()) {
            writer.addAlignment(iterator.next());
            count++;
        }

        return count;
    }

    /*
     * @see edu.cshl.schatz.jnomics.mapreduce.JnomicsTool#getOptions()
     */
    @Override
    public Options getOptions() {
        OptionBuilder optionBuilder = new OptionBuilder();
        Options options = new Options();

        /** @formatter:off */            
        options.addOption(optionBuilder
            .withLongOpt(OPTION_IN_LONG)
            .withArgName("stdin|uri")
            .withDescription("The input file. (stdin)")
            .hasArgs(1)
            .isRequired(false)
            .create(OPTION_IN));

        options.addOption(optionBuilder
            .withLongOpt(OPTION_OUT_LONG)
            .withArgName("stdout|uri")
            .withDescription("The output destination (stdout)")
            .hasArgs(1)
            .isRequired(false)
            .create(OPTION_OUT));

        options.addOption(optionBuilder
            .withLongOpt(OPTION_NOCONVERT_LONG)
            .withDescription("Just stream the file to the target; do not attempt to convert.")
            .isRequired(false)
            .create(OPTION_NOCONVERT));

        options.addOption(optionBuilder
            .withLongOpt(OPTION_PATHS_LONG)
            .withDescription("Only print the input and output URIs to stdout; do not stream.")
            .isRequired(false)
            .create(OPTION_PATHS));

        options.addOption(optionBuilder
            .withLongOpt(OPTION_VERBOSE_LONG)
            .withDescription("Verbose operation.")
            .isRequired(false)
            .create(OPTION_VERBOSE));
        /** @formatter:on */

        return options;
    }

    /*
     * @see
     * edu.cshl.schatz.jnomics.mapreduce.JnomicsTool#handleOptions(org.apache
     * .commons.cli.CommandLine)
     */
    @Override
    public int handleOptions(CommandLine cmd) throws ParseException, IOException {
        String inName, outName;
        Path inPath = null, outPath = null;

        optionNoConvert = cmd.hasOption(OPTION_NOCONVERT);
        optionPathsOnly = cmd.hasOption(OPTION_PATHS);
        optionVerbose = optionPathsOnly || cmd.hasOption(OPTION_VERBOSE);

        inName = cmd.getOptionValue(OPTION_IN);
        outName = cmd.getOptionValue(OPTION_OUT);

        try {
            if (inName == null) {
                in = System.in;
                inName = "stdin";
            } else {
                // String uri = interpretURI(inName, conf);
                FileSystem fs = FileSystem.get(new URI(inName), getConf());

                inPath = new Path(inName).makeQualified(fs);
                inName = inPath.toUri().toString();

                if (!optionPathsOnly) {
                    in = fs.open(inPath);
                }
            }

            if (outName == null) {
                out = System.out;
                outName = "stdout";
            } else {
                // String uri = interpretURI(outName, conf);
                FileSystem fs = FileSystem.get(new URI(outName), getConf());

                outPath = new Path(outName).makeQualified(fs);
                outName = outPath.toUri().toString();

                if (!optionPathsOnly) {
                    out = fs.create(outPath);
                }
            }

            if (optionVerbose) {
                System.out.printf("in=%s%n", inName);
                System.out.printf("out=%s%n", outName);
            }

            if (!optionPathsOnly && ((inPath == outPath) || inPath.equals(outPath))) {
                throw new ParseException("In and out paths are the same.");
            }
        } catch (URISyntaxException e) {
            throw new ParseException(e.getLocalizedMessage());
        }

        return STATUS_OK;
    }

    @Override
    public int run(String[] args) throws IOException {
        if (!optionPathsOnly) {
            String pattern;
            int count = 0;
            long time = System.currentTimeMillis();

            if (optionNoConvert) {
                // Do not convert: just copy the data.
                int read;
                byte[] bytes = new byte[4096];

                while (-1 != (read = in.read(bytes))) {
                    out.write(bytes, 0, read);
                    count += read;
                }

                pattern = count + " bytes copied in %dh%dm%d.%03ds%n";
            } else {
                ConvertCopy filter = new ConvertCopy();

                count = filter.doBAMConvertCopy(in, out);
                pattern = count + " records convert-copied in %dh%dm%d.%03ds%n";
            }

            time = System.currentTimeMillis() - time;

            long hours = time / (1000 * 60 * 60);
            long minutes = (time %= 60) / (1000 * 60);
            long seconds = (time %= 60) / (1000);
            long millis = time % 1000;

            System.out.printf(pattern, hours, minutes, seconds, millis);
        }

        return STATUS_OK;
    }

    /*
     * @see
     * edu.cshl.schatz.jnomics.mapreduce.JnomicsTool#buildDefaultJnomicsOptions
     * (org.apache.commons.cli.Options, java.lang.String[])
     */
    @Override
    protected Options buildDefaultJnomicsOptions(Options options, String... exclude) {
        return super.buildDefaultJnomicsOptions(options, new String[] { "in", "out" });
    }
}
