/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

import edu.cshl.schatz.jnomics.cli.OptionBuilder;
import edu.cshl.schatz.jnomics.io.FileFormatException;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsTool;
import edu.cshl.schatz.jnomics.ob.writable.QueryTemplate;
import edu.cshl.schatz.jnomics.ob.writable.SequencingRead;

/**
 * <p>
 * This is class performs the initial read alignment sorting and pre-processing
 * in the Hydra pipeline. Specifically, it combines and parallelizes the
 * functions of <code>samtools sort</code>, <code>fastq_quality_trimmer</code>,
 * <code>matchpairs.pl</code>, and the discordant pair finding role of
 * <code>samtools view</code>.
 * </p>
 * 
 * @author Matthew Titmus
 */
public class AlignmentProcessor extends JnomicsTool {
    public static final String CMD_DESCRIPTION = "Read alignment preprocessor and sorter.";

    public static final String CMD_HEADING = CMD_DESCRIPTION + "\n"
            + "2011, Matthew Titmus (mtitmus@cshl.edu)";

    public static final String CMD_NAME = "processor";

    public static final String CMD_USAGE = CMD_NAME
            + " [options] -in <file> -out <path> -fin <format>";

    public static final String P_PROC_COMPLEMENTATION = "jnomics.processor.complementation";

    public static final String P_PROC_FILTER_FLAGS = "jnomics.processor.flags.filter";

    public static final String P_PROC_FORMAT_OUT = "jnomics.processor.format.out";

    public static final String P_PROC_MIN_LEN = "jnomics.processor.minlength";

    public static final String P_PROC_NAMES_KEEP = "jnomics.processor.names.keep";

    public static final String P_PROC_OFFSET_IN = "jnomics.processor.offset.in";

    public static final String P_PROC_OFFSET_OUT = "jnomics.processor.offset.out";

    // Not yet implemented:
    // static final String OPT_FILTER_NAME = "N";
    // static final String OPT_FILER_RANGE = "R";
    // static final String OPT_KEEP_FLAGS = "f";

    public static final String P_PROC_QUAL_THRESHOLD = "jnomics.processor.quality.threshold";
    public static final String P_PROC_RANGES_KEEP = "jnomics.processor.ranges.keep";
    public static final String P_PROC_REQUIRE_FLAGS = "jnomics.processor.flags.require";
    public static final String RANGE_DELIM = " ";
    public static final String RANGE_FIELD_DELIM = "@@";

    static final Log LOG = LogFactory.getLog(AlignmentProcessor.class);

    static final String OPT_COMPLEMENTATION = "c";

    static final String OPT_FILTER_FLAGS = "F";

    static final String OPT_KEEP_NAME = "n";

    static final String OPT_KEEP_RANGE = "r";

    static final String OPT_LENGTH_MIN = "l";

    static final String OPT_QUALITY_OFFSET = "Q";

    static final String OPT_QUALITY_THRESHOLD = "t";

    static final String OPT_REQUIRE_FLAGS = "f";

    public AlignmentProcessor() {
        setHelpUsage(CMD_USAGE);
        setHelpHeading(CMD_DESCRIPTION);
    }

    public static void main(String[] args) throws Exception {
        JnomicsTool.run(new AlignmentProcessor(), args);
    }

    /**
     * @param optionName
     * @param cmd
     * @return An [int|Integer] containing the option value, or
     *         <code>null</code> if the value isn't set.
     * @throws NumberFormatException if the value isn't an integer.
     */
    private static Integer getIntegerOptionValue(String optionName, CommandLine cmd)
            throws ParseException {

        Integer value = null;
        String valueString;

        try {
            if (null != (valueString = cmd.getOptionValue(optionName))) {
                value = Integer.parseInt(valueString);
            }
        } catch (NumberFormatException e) {
            throw new ParseException("Option " + optionName + " expects a number; got "
                    + cmd.getOptionValue(optionName));
        }

        return value;
    }

    /**
     * Reads in one or more "range files", and returns the contents in the form
     * of a String of ranges to be used as a configuration property.
     */
    private static String readRangeFiles(String[] rangefileNames) throws IOException {
        StringBuilder sb = null;

        for (String name : rangefileNames) {
            File f = new File(name);

            if (!f.exists()) {
                throw new FileNotFoundException("File not found: " + f.getCanonicalPath());
            } else {
                BufferedReader in = new BufferedReader(new FileReader(f));
                String line, parts[];

                while (null != (line = in.readLine())) {
                    // "\p{Space}+" is the POSIX character classes for
                    // "any whitespace character": [ \t\n\x0B\f\r]

                    parts = line.split("\\p{Space}+");

                    if (parts.length != 3) {
                        throw new FileFormatException("Expected: \"chr int int\"; Got: \"" + line
                                + "\"");
                    } else if (sb != null) {
                        sb.append(RANGE_DELIM + parts[0] + RANGE_FIELD_DELIM + parts[1]
                                + RANGE_FIELD_DELIM + parts[2]);
                    } else {
                        sb = new StringBuilder(parts[0] + RANGE_FIELD_DELIM + parts[1]
                                + RANGE_FIELD_DELIM + parts[2]);
                    }
                }
            }
        }

        return sb.toString();
    }

    /**
     * takes input as a comma separated list of files and verifies if they
     * exist. It defaults for file:/// if the files specified do not have a
     * scheme. it returns the paths uri converted defaulting to file:///. So an
     * input of /home/user/file1,/home/user/file2 would return
     * file:///home/user/file1,file:///home/user/file2
     */
    private static String validateFiles(String files, Configuration conf) throws IOException {
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
                // default to the local file system check if the file exists
                if (!localFs.exists(path)) {
                    throw new FileNotFoundException("File " + tmp + " does not exist.");
                }

                finalPath = path.makeQualified(localFs).toString();
            } else {
                // check if the file exists in this file system we need to
                // recreate this filesystem object to copy these files to the
                // file system jobtracker is running on.
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

    /*
     * @see edu.cshl.schatz.jnomics.mapreduce.JnomicsTool#getOptions()
     */
    @Override
    public Options getOptions() {
        OptionBuilder optionBuilder = new OptionBuilder();
        Options options = new Options();

        /** @formatter:off */
        options.addOption(optionBuilder
            .withArgName("offset [new]")
            .withDescription("Handle SAM ASCII quality with the specified offset. "
                    + "Default = 64 (Illumina, Solexa). For Sanger reads this should be "
                    + "set to 33. The optional second value directs the processor to "
                    + "adjust the phred string to new the offset value.")
            .hasArgs(2)
            .isRequired(false)
            .withWeight(10)
            .create(OPT_QUALITY_OFFSET));

        options.addOption(optionBuilder
            .withArgName("str")
            .withDescription("Complementation behavior. Valid options are: "
                    + "none (do nothing), reverse (reverse all reads)")
            .hasArg()
            .isRequired(false)
            .withWeight(10)
            .create(OPT_COMPLEMENTATION));

        options.addOption(optionBuilder
            .withArgName("int")
            .withDescription("Quality threshold - if set, nucleotides with "
                    + "lower quality will be trimmed (from the end "
                    + "of the sequence). Valid values are " + "0-93, inclusive.")
            .hasArg()
            .isRequired(false)
            .withWeight(10)
            .create(OPT_QUALITY_THRESHOLD));

        options.addOption(optionBuilder
            .withArgName("int")
            .withDescription("Minimum length - sequences shorter "
                    + "than this (after trimming) will be discarded. "
                    + "0 = no minimum length. [0]")
            .hasArg()
            .isRequired(false)
            .withWeight(10)
            .create(OPT_LENGTH_MIN));

        options.addOption(optionBuilder
            .withArgName("file(s)")
            .withDescription("Required ranges - keep only reads aligned within "
                    + "the given range(s). Expects comma-delimited list of files; "
                    + "each should contain one range per line: \'chromosome start end.\'")
            .hasArg()
            .isRequired(false)
            .withWeight(10)
            .create(OPT_KEEP_RANGE));

        options.addOption(optionBuilder
            .withArgName("file")
            .withDescription("Required names - keep only read template names "
                    + "matching given pattern(s). Expects comma-delimited list of files; "
                    + "one regular expression per line")
            .hasArg()
            .isRequired(false)
            .withWeight(10)
            .create(OPT_KEEP_NAME));

        options.addOption(optionBuilder
            .withArgName("int")
            .withDescription("Flag filter - Reads matching the "
                    + "bitwise flag value are discarded; 0 is unset. [0]")
            .hasArg()
            .isRequired(false)
            .withWeight(10)
            .create(OPT_FILTER_FLAGS));

        options.addOption(optionBuilder
            .withArgName("int")
            .withDescription("Flag require - Reads NOT matching the "
                    + "bitwise flag value are discarded; 0 is unset. [0]")
            .hasArg()
            .isRequired(false)
            .withWeight(10)
            .create(OPT_REQUIRE_FLAGS));
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
        Configuration conf = getConf();

        conf.set(P_PROC_COMPLEMENTATION, cmd.getOptionValue(OPT_COMPLEMENTATION, "none"));

        Integer value;
        String[] offsets, rangefiles;

        if (null != (value = getIntegerOptionValue(OPT_FILTER_FLAGS, cmd))) {
            conf.setInt(P_PROC_FILTER_FLAGS, value);
        }

        if (null != (value = getIntegerOptionValue(OPT_REQUIRE_FLAGS, cmd))) {
            conf.setInt(P_PROC_REQUIRE_FLAGS, value);
        }

        if (null != (offsets = cmd.getOptionValues(OPT_QUALITY_OFFSET))) {
            for (int i = 0; i < offsets.length; i++) {
                int offset = Integer.parseInt(offsets[i]);

                if ((offset < 0) || (offset > 93)) {
                    throw new ParseException("Quality thresholds " + "must range from 0..93");
                }

                switch (i) {
                case 0:
                    conf.setInt(P_PROC_OFFSET_IN, offset);
                    break;
                case 1:
                    conf.setInt(P_PROC_OFFSET_OUT, offset);
                    break;
                }
            }
        }

        if (null != (value = getIntegerOptionValue(OPT_QUALITY_THRESHOLD, cmd))) {
            if ((value < 0) || (value > 93)) {
                throw new ParseException("Quality thresholds " + "must range from 0..93");
            }

            conf.setInt(OPT_QUALITY_THRESHOLD, value);
        }

        if (null != (value = getIntegerOptionValue(OPT_LENGTH_MIN, cmd))) {
            conf.setInt(OPT_QUALITY_THRESHOLD, value);
        }

        if (null != (rangefiles = cmd.getOptionValues(OPT_KEEP_RANGE))) {
            conf.set(P_PROC_RANGES_KEEP, readRangeFiles(rangefiles));
        }

        // Lists of names may get long, so we upload the entire list of
        // files, and place the file names into a property
        String nameProp, nameFiles;
        if (null != (nameFiles = cmd.getOptionValue(OPT_KEEP_NAME))) {
            final String TMP_FILES_PROP = "tmpfiles";

            try {
                nameFiles = validateFiles(nameFiles, conf);
                conf.set(P_PROC_NAMES_KEEP, nameFiles);

                // The mapreduce framework automatically uploads any files
                // listed in the "tmpfiles" attribute to the distributed
                // filesystem.
                if (null == (nameProp = conf.get(TMP_FILES_PROP))) {
                    conf.set(TMP_FILES_PROP, nameFiles);
                } else {
                    conf.set(TMP_FILES_PROP, nameProp + "," + nameFiles);
                }
            } catch (FileNotFoundException e) {
                throw new ParseException(e.getLocalizedMessage());
            }
        }

        return STATUS_OK;
    }

    /*
     * @see
     * edu.cshl.schatz.jnomics.mapreduce.JnomicsTool#run(java.lang.String[])
     */
    @Override
    public int run(String[] args) throws Exception {
        int status = 1;

        if (STATUS_OK == handleParameters(args)) {
            getJob().setJarByClass(AlignmentProcessor.class);
            getJob().setReducerClass(AlignmentReducer.class);
            getJob().setCombinerClass(AlignmentReducer.class);

            status = getJob().waitForCompletion(true) ? 0 : 1;
        }

        return status;
    }

    public static class AlignmentReducer
            extends JnomicsReducer<Writable, QueryTemplate, Writable, QueryTemplate> {

        static final int DEFAULT_FLAGS = 0;
        static final int DEFAULT_MIN_LENGTH = 0;
        static final int DEFAULT_OFFSET = 33;
        static final int DEFAULT_QUALITY_THRESHOLD = -1;

        private ComplementationBehavior complementationBehavior = null;

        private int countFilteredByFlags = 0;
        private int countFilteredByName = 0;
        private int countFilteredByRange = 0;
        private int countQCFails = 0;
        private int countTotalReadsIn = 0;
        private int countTotalReadsOut = 0;
        private int flagsFiltered, flagsRequired;

        private Map<String, List<int[]>> keepRangeMap = null;

        private int minimumLength;

        private Pattern namePattern = null;

        private int offsetIn = DEFAULT_OFFSET, offsetOut = DEFAULT_OFFSET, offsetModifier = 0;

        private int qualityThreshold = DEFAULT_QUALITY_THRESHOLD;

        /*
         * @see
         * org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce
         * .Mapper.Context)
         */
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            String counterName = this.getClass().getSimpleName();
            context.getCounter(counterName, "Reads in").increment(countTotalReadsIn);
            context.getCounter(counterName, "QC Failure").increment(countQCFails);
            context.getCounter(counterName, "Filtered by flags").increment(countFilteredByFlags);
            context.getCounter(counterName, "Reads out").increment(countTotalReadsOut);

            if (keepRangeMap != null) {
                context.getCounter(counterName, "Filtered by position").increment(
                    countFilteredByRange);
            }

            if (namePattern != null) {
                context.getCounter(counterName, "Filtered by name").increment(countFilteredByName);
            }
        }

        /*
         * @see
         * org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce
         * .Mapper.Context)
         */
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            Configuration config = context.getConfiguration();

            flagsFiltered = config.getInt(P_PROC_FILTER_FLAGS, DEFAULT_FLAGS);
            flagsRequired = config.getInt(P_PROC_REQUIRE_FLAGS, DEFAULT_FLAGS);
            minimumLength = config.getInt(P_PROC_MIN_LEN, DEFAULT_MIN_LENGTH);
            offsetIn = config.getInt(P_PROC_OFFSET_IN, DEFAULT_OFFSET);
            offsetOut = config.getInt(P_PROC_OFFSET_OUT, DEFAULT_OFFSET);
            offsetModifier = offsetOut - offsetIn;
            qualityThreshold = config.getInt(P_PROC_QUAL_THRESHOLD, DEFAULT_QUALITY_THRESHOLD);

            // If the P_PROC_NAMES_KEEP property is set (directs the mapper to
            // keep only reads with names that match a set of regular
            // expressions), we read the files and build a single large regular
            // expression here.

            String namesProperty;
            if (null != (namesProperty = config.get(P_PROC_NAMES_KEEP))) {
                String[] nameFiles = namesProperty.split(",");
                StringBuilder b = null;

                for (String nameFile : nameFiles) {
                    try {
                        FileSystem fs = FileSystem.get(new URI(nameFile), config);
                        Path nameFilePath = new Path(nameFile);
                        String rawName, regex;

                        BufferedReader in = new BufferedReader(new InputStreamReader(
                            fs.open(nameFilePath)));

                        while (null != (rawName = in.readLine())) {
                            if (((rawName = rawName.trim()).length() == 0)
                                    || (rawName.charAt(0) == '#')) {
                                continue;
                            }

                            regex = NameToRegexConverter.convert(rawName);

                            if (b == null) {
                                b = new StringBuilder("^((" + regex + ")");
                            } else {
                                b.append("|(" + regex + ")");
                            }
                        }
                    } catch (URISyntaxException urie) {
                        throw new IOException(urie);
                    }
                }

                b.append(")$");

                namePattern = Pattern.compile(b.toString());
            }

            // If the P_PROC_RANGES_KEEP property is set (directs the mapper to
            // keep only reads that are mapped to defined ranges of particular
            // chromosomes), we build a map of chromosomes to lists of ranges
            // here.

            String rangeProperty;
            if (null != (rangeProperty = config.get(P_PROC_RANGES_KEEP))) {
                String[] thisRange;
                List<int[]> rangeList;

                keepRangeMap = new HashMap<String, List<int[]>>();

                for (String r : rangeProperty.split(RANGE_DELIM)) {
                    thisRange = r.split(RANGE_FIELD_DELIM);

                    if (null == (rangeList = keepRangeMap.get(thisRange[0]))) {
                        keepRangeMap.put(thisRange[0], rangeList = new ArrayList<int[]>(5));
                    }

                    rangeList.add(new int[] {
                            Integer.parseInt(thisRange[1]),
                            Integer.parseInt(thisRange[2]) });
                }
            }

            String complementProperty;
            if (null != (complementProperty = config.get(P_PROC_COMPLEMENTATION))) {
                complementProperty = complementProperty.toLowerCase();

                complementationBehavior = null;
                for (ComplementationBehavior b : ComplementationBehavior.values()) {
                    if (complementProperty.equals(b.arg)) {
                        complementationBehavior = b;
                        break;
                    }
                }
            }

            if (complementationBehavior == null) {
                String msg = String.format(
                    "Unknown value for %s: %s. Defaulting to 'none'", P_PROC_COMPLEMENTATION,
                    config.get(P_PROC_COMPLEMENTATION));

                LOG.error(msg);
            } else if (complementationBehavior == ComplementationBehavior.NONE) {
                complementationBehavior = null; // NULL == "none"
            }
        }

        /*
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN,
         * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        @Override
        protected void reduce(Writable key, Iterable<QueryTemplate> values, Context context)
                throws IOException, InterruptedException {

            for (QueryTemplate qt : values) {
                // If "filter by template name" is being used, check name here.
                if ((null != namePattern)
                        && namePattern.matcher(qt.getTemplateNameString()).matches()) {
                    countFilteredByName += qt.size();
                }

                for (int i = 0; i < qt.size(); i++) {
                    SequencingRead sequencingRead = qt.get(i);
                    countTotalReadsIn++;

                    if (offsetModifier != 0) {
                        adjustPhredOffset(sequencingRead, offsetModifier);
                    }

                    if (qualityThreshold >= 0) {
                        sequencingRead.trimByQuality(qualityThreshold, offsetOut);
                    }

                    if (sequencingRead.length() <= minimumLength) {
                        countQCFails++;
                        qt.remove(i);
                        continue;
                    }

                    if (!sequencingRead.hasAllFlags(flagsRequired)
                            || ((flagsFiltered != 0) && sequencingRead.hasAnyFlags(flagsFiltered))) {

                        countFilteredByFlags++;
                        qt.remove(i);
                        continue;
                    }

                    // If the keep range map isn't null and contains a list
                    // of ranges for the reference sequence name (which would be
                    // a chromosome for a human alignment), we check the entry
                    // against each range value in the list and keep it if it
                    // falls within one.
                    //
                    // if (null != keepRangeMap) {
                    // List<int[]> ranges;
                    // boolean isOutsideRange = true;
                    //
                    // if (null != (ranges =
                    // keepRangeMap.get(entry.getColumn(SAMColumn.REF_NAME)))) {
                    // int[] range; // The range array (0=start; 1=end).
                    //
                    // entryRange.setEndpoints(entry.getPositionFirst(),
                    // entry.getPositionLast());
                    //
                    // for (int i = 0; (i < ranges.size()) && isOutsideRange;
                    // i++) {
                    // range = ranges.get(i);
                    // filterRange.setEndpoints(range[0], range[1]);
                    // entryRange.overlap(filterRange);
                    //
                    // // filterRange.length() is now set to the number
                    // // of bases shared by the ranges. If this value
                    // // is non-positive, there's no overlap.
                    // isOutsideRange = (filterRange.length() <= 0);
                    // }
                    // }
                    //
                    // // We didn't fall inside any ranges. Bail.
                    // if (isOutsideRange) {
                    // sequencingReadIterator.remove();
                    // filteredByRange++;
                    // }
                    // }

                    if (complementationBehavior != null) {
                        switch (complementationBehavior) {
                        case NONE:
                            break;
                        case REVERSE:
                            sequencingRead.reverseComplement();
                            break;
                        }
                    }
                }

                if (qt.size() > 0) {
                    context.write(qt.getTemplateName(), qt);
                    countTotalReadsOut += qt.size();
                }
            }
        }

        /**
         * Adjusts the phred score offset value by the indicated value. For
         * example, if a read is using a Phred format of Phred+64, to convert to
         * Phred+33 the parameter would be -31 = (33 - 64).
         */
        private void adjustPhredOffset(SequencingRead read, int adjustBy) {
            int phredLength = read.getPhredString().getLength();
            byte[] phred = read.getPhredString().getBytes();

            for (int i = 0; i < phredLength; i++) {
                phred[i] = (byte) (phred[i] + adjustBy);
            }
        }
    }

    /**
     * Converts wild-card characters into their equivalent regex codes, and
     * scrubs existing non-wildcard regex characters by inserting backslashes.
     */
    public static class NameToRegexConverter {
        static final String MATCH_MANY_PATTERN = "([^\\\\])\\\\\\*";
        static final String MATCH_MANY_REPLACE = "$1.*";

        static final String MATCH_ONE_PATTERN = "([^\\\\])\\\\\\?";
        static final String MATCH_ONE_REPLACE = "$1.";

        static final Pattern matchManyPattern = Pattern.compile(MATCH_MANY_PATTERN);
        static final Pattern matchOnePattern = Pattern.compile(MATCH_ONE_PATTERN);

        static final String SCRUB_PATTERN = "([\\\\\\|\\?\\*\\$\\^\\{\\}\\(\\)\\.\\+])";
        static final String SCRUB_REPLACE = "\\\\$1";
        static final Pattern scrubPattern = Pattern.compile(SCRUB_PATTERN);

        public static final String convert(String name) {
            name = scrubPattern.matcher(name).replaceAll(SCRUB_REPLACE);
            name = matchOnePattern.matcher(name).replaceAll(MATCH_ONE_REPLACE);
            name = matchManyPattern.matcher(name).replaceAll(MATCH_MANY_REPLACE);

            return name;
        }
    }

    /**
     * Used to specify the outgoing sort method.
     * 
     * @author Matthew A. Titmus
     */
    public static enum SortOn {
        COORDINATE,

        QUERYNAME,

        /** Default: mapper keeps existing key. */
        UNKNOWN,

        UNSORTED;

        public static SortOn getSortOn(String name) {
            return getSortOn(name, null);
        }

        public static SortOn getSortOn(String name, SortOn defaultSortOn) {
            name = name.toUpperCase();

            for (SortOn sortOn : SortOn.values()) {
                if (name.equals(sortOn.name())) {
                    return sortOn;
                }
            }

            return defaultSortOn;
        }
    }

    private static enum ComplementationBehavior {
        // Do nothing (default)
        NONE("none"),

        // Reverse complement all reads
        REVERSE("reverse");

        final String arg;

        ComplementationBehavior(String arg) {
            this.arg = arg;
        }
    }
}
