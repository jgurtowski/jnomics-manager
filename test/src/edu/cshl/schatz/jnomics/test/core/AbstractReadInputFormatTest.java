/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.test.core;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.regex.Pattern;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.cshl.schatz.jnomics.io.SequencingReadInputFormat;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsJob;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsMapper;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer;
import edu.cshl.schatz.jnomics.mapreduce.ReadFileFormat;
import edu.cshl.schatz.jnomics.ob.Orientation;
import edu.cshl.schatz.jnomics.ob.writable.QueryTemplate;
import edu.cshl.schatz.jnomics.ob.writable.SequencingRead;

/**
 * @author Matthew Titmus
 */
abstract class AbstractReadInputFormatTest extends TestCase {
    public static final String FULL_FILE_PATH = "test-data/inputFormats/";

    public static final String SINGLE_LINE_FILE_PATH = "test-data/inputFormats-1-line/";

    static final Log LOG = LogFactory.getLog(AbstractReadInputFormatTest.class);

    private static final Pattern TRAILING_SLASH_PATTERN = Pattern.compile("/[0-9]$");

    final long FINAL_EXPECTED_TEMPLATES = 1000, FINAL_EXPECTED_READS = 2000;
    long expectedTemplates, expectedReads;

    boolean initialized = false;

    private long templateCount, reducerCount, _inFileSize = 0;

    static void validateRead(QueryTemplate template, SequencingRead read) {
        String templateName = template.getTemplateName() == null
                ? null
                : template.getTemplateName().toString();
        String readName = read.getReadName() == null ? null : read.getReadName().toString();
        String expectedName = templateName + (read.isFirst() ? "/1" : "/2");

        final String prefix = "[Template=" + templateName + "; Read=" + readName + "] ";

        { // First, check for NULL names
            assertNotNull(prefix + "NULL template name", templateName);
            assertNotNull(prefix + "NULL read name", readName);
            assertNotNull(prefix + "NULL query template", read.getQueryTemplate());

            assertFalse(prefix + "EMPTY template name", templateName.length() == 0);
            assertFalse(prefix + "EMPTY query template", template.size() == 0);
        }

        { // Second, check for expected read values
            String errorMsg = prefix + "Unexpected first/last flags (first=" + read.isFirst()
                    + "; last=" + read.isLast() + "; flags=0x"
                    + Integer.toHexString(read.getFlags());

            assertTrue(errorMsg, read.isFirst() ^ read.isLast());

            assertFalse(
                prefix + "Unspecified orientation.",
                read.getOrientation() == Orientation.UNSPECIFIED);

            assertTrue(
                prefix + "Orientation/flag disagrement",
                read.isReverseComplemented() == (read.getOrientation() == Orientation.MINUS));
        }

        { // Third, check that the read has a trailing trailing
          // slash-number, and that the template does not.
            assertFalse(
                prefix + "Template name has trailing slash-number",
                TRAILING_SLASH_PATTERN.matcher(templateName).find());

            assertEquals(
                prefix + "Read name is missing trailing slash-number", expectedName, readName);
        }
    }

    /**
     * @return The expectedReadFileFormat
     */
    public abstract ReadFileFormat getExpectedReadFileFormat();

    /**
     * @return The inPaths
     */
    public abstract Path[] getFullFileInPaths();

    /**
     * @return The mapperClass
     */
    public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass() {
        return ReadInputTestMapper.class;
    }

    /**
     * @return The outPath
     */
    public Path getOutPath() {
        return new Path("/tmp/readInputFormatTest/" + getExpectedReadFileFormat().extension);
    }

    /**
     * @return The reducerClass
     */
    public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass() {
        return ReadInputTestReducer.class;
    }

    /**
     * @return The inPaths
     */
    public abstract Path[] getSingleLineFilePaths();

    public void testFileType() throws Exception {
        for (Path inPath : getFullFileInPaths()) {
            assertEquals(ReadFileFormat.get(inPath), getExpectedReadFileFormat());
        }
    }

    public void testFiveSplits() throws Exception {
        init();

        runSplitTest((int) (0.5 + (getInfileSize() / 5.0)));
    }

    public void testNoSplits() throws Exception {
        init();

        runSplitTest(getInfileSize());
    }

    public void testOneHundredSplits() throws Exception {
        init();

        runSplitTest((int) (0.5 + (getInfileSize() / 100.0)));
    }

    public void testTwoSplits() throws Exception {
        init();

        runSplitTest((int) (0.5 + (getInfileSize() / 2.0)));
    }

    Job buildTestJob(long splitSize)
            throws IOException, InterruptedException, ClassNotFoundException {

        Job job = new JnomicsJob();
        Configuration conf = job.getConfiguration();

        conf.setLong("mapred.max.split.size", splitSize);

        FileOutputFormat.setOutputPath(job, getOutPath());
        FileSystem fs = getOutPath().getFileSystem(conf);

        for (Path inPath : getFullFileInPaths()) {
            FileInputFormat.addInputPath(job, inPath);
        }

        if (fs.exists(getOutPath())) {
            fs.delete(getOutPath(), true);
        }

        job.setInputFormatClass(SequencingReadInputFormat.class);

        if (getMapperClass() != null) {
            job.setMapperClass(getMapperClass());
        }

        if (getReducerClass() != null) {
            job.setReducerClass(getReducerClass());
        }

        return job;
    }

    private long getInfileSize() throws IOException {
        if (_inFileSize < 1) {
            long tmpSize = 0;

            FileSystem fs;
            for (Path inPath : getFullFileInPaths()) {
                fs = inPath.getFileSystem(new Job().getConfiguration());
                tmpSize += fs.getFileStatus(inPath).getLen();
            }

            _inFileSize = tmpSize;
        }

        return _inFileSize;
    }

    private void init() throws Exception {
        final String FILTER_TEXT = "^.+(No job jar file set|GenericOptionsParser|"
                + "INFO (jvm\\.JvmMetrics|input\\.FileInputFormat|"
                + "mapred\\.(Merger|MapTask|TaskRunner|LocalJobRunner))).+[\n]?";

        if (!initialized) {
            System.setErr(new FilteringPrintStream(System.err, FILTER_TEXT));

            Job job = buildTestJob(Integer.MAX_VALUE);

            job.waitForCompletion(false);

            CounterGroup countGroup = job.getCounters().getGroup("Test Reducer");
            Counter templateCounter = countGroup.findCounter("Templates");
            Counter readsCounter = countGroup.findCounter("Reads");

            expectedTemplates = templateCounter.getValue();
            expectedReads = readsCounter.getValue();

            assertTrue("Initialization got zero templates", expectedTemplates > 0);
            assertTrue("Initialization got zero reads", expectedReads > 0);

            System.out.println("INIT: Found " + expectedReads + " reads in " + expectedTemplates
                    + " templates.");

            System.out.printf(
                "INITIALIZATION: Templates=%d; Reads=%d%n", expectedTemplates, expectedReads);

            initialized = true;
        }
    }

    private void runSplitTest(long splitSize) throws Exception {
        init();

        assertEquals("FormatTest has not been properly initialized.", true, initialized);

        Job job = buildTestJob(splitSize);

        assertEquals(true, job.waitForCompletion(true));

        CounterGroup mapperCountGroup = job.getCounters().getGroup("Test Mapper");
        Counter mapperTemplateCounter = mapperCountGroup.findCounter("Templates");
        Counter mapperReadsCounter = mapperCountGroup.findCounter("Reads");

        templateCount = mapperTemplateCounter.getValue();
        reducerCount = mapperReadsCounter.getValue();

        System.out.printf(
            "MAPPER:  Templates=%d (expected %d); Reads=%d (expected %d)%n", templateCount,
            expectedTemplates, reducerCount, expectedTemplates);

        CounterGroup reducerCountGroup = job.getCounters().getGroup("Test Reducer");
        Counter reducerTemplateCounter = reducerCountGroup.findCounter("Templates");
        Counter reducerReadsCounter = reducerCountGroup.findCounter("Reads");

        templateCount = reducerTemplateCounter.getValue();
        reducerCount = reducerReadsCounter.getValue();

        System.out.printf(
            "REDUCER: Templates=%d (expected %d); Reads=%d (expected %d)%n", templateCount,
            FINAL_EXPECTED_TEMPLATES, reducerCount, FINAL_EXPECTED_READS);

        assertEquals(expectedTemplates, templateCount);
        assertEquals(expectedReads, reducerCount);
    }

    public static class ReadInputTestMapper
            extends JnomicsMapper<Writable, QueryTemplate, Writable, QueryTemplate> {

        int templateCount = 0, readCount = 0;

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.getCounter("Test Mapper", "Templates").increment(templateCount);
            context.getCounter("Test Mapper", "Reads").increment(readCount);
        }

        @Override
        protected void map(Writable key, QueryTemplate template, Context context)
                throws IOException, InterruptedException {

            for (SequencingRead read : template) {
                validateRead(template, read);
            }

            templateCount++;

            String templateName = template.getTemplateName() == null
                    ? null
                    : template.getTemplateName().toString();

            readCount += template.size();

            assertTrue("[Template=" + templateName + "] ", template.size() <= 2);

            context.write(key, template);
        }
    }

    public static class ReadInputTestReducer
            extends JnomicsReducer<Writable, QueryTemplate, Writable, QueryTemplate> {

        /*
         * @see
         * org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce
         * .Reducer.Context)
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            emptyCounter = 0;

        }

        int templateCount = 0, readCount = 0;
        int emptyCounter = 0;

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            final String groupName = "Test Reducer";

            context.getCounter(groupName, "Templates").increment(templateCount);
            context.getCounter(groupName, "Reads").increment(readCount);
        }

        /*
         * @see
         * edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer#reduce(java.lang
         * .Object, java.lang.Iterable,
         * org.apache.hadoop.mapreduce.Reducer.Context)
         */
        @Override
        protected void reduce(Writable key, Iterable<QueryTemplate> templates, Context context)
                throws IOException, InterruptedException {

            for (QueryTemplate template : templates) {
                String templateName = template.getTemplateName() == null
                        ? null
                        : template.getTemplateName().toString();

                templateCount++;
                readCount += template.size();

                if (template.size() == 0) {
                    System.out.println("EMPTY TEMPLATE #" + (++emptyCounter) + ": name=\""
                            + templateName + "\"");
                    context.getCounter("TESTING", "Templates").increment(templateCount);
                }

                for (SequencingRead read : template) {
                    assertNotNull(read.getQueryTemplate());

                    validateRead(template, read);
                }

                assertEquals("[Template=" + templateName + "] ", 2, template.size());
                context.write(template.getTemplateName(), template);
            }
        }
    }

    /**
     * A {@link PrintStream} subclass that filters writes based on regular
     * expression matching. I use it here to reduce the Hadoop framework noise
     * without having to mess around with configuration files. Not efficient at
     * all; don't use this in production.
     * 
     * @author Matthew Titmus
     */
    static class FilteringPrintStream extends PrintStream {
        private final Pattern filter;

        private final Pattern keep;

        /**
         * @param out
         */
        public FilteringPrintStream(OutputStream out, String filterRegex) {
            this(out, filterRegex, null);
        }

        /**
         * @param out
         */
        public FilteringPrintStream(OutputStream out, String filterRegex, String keepRegex) {
            super(out);

            if (filterRegex != null) {
                filter = Pattern.compile(filterRegex);
            } else {
                filter = null;
            }

            if (keepRegex != null) {
                keep = Pattern.compile(keepRegex);
            } else {
                keep = Pattern.compile(".*"); // Match everything
            }
        }

        /*
         * @see java.io.PrintStream#append(char)
         */
        @Override
        public PrintStream append(char c) {
            return super.append(c);
        }

        /*
         * @see java.io.PrintStream#print(java.lang.String)
         */
        @Override
        public void print(String x) {
            if ((filter == null) || !filter.matcher(x).matches() || keep.matcher(x).matches()) {
                super.print(x);
            }
        }

        /*
         * @see java.io.PrintStream#print(java.lang.String)
         */
        @Override
        public void println(String x) {
            if ((filter == null) || !filter.matcher(x).matches() || keep.matcher(x).matches()) {
                super.println(x);
            }
        }

        /*
         * @see java.io.PrintStream#write(byte[], int, int)
         */
        @Override
        public void write(byte[] buf, int off, int len) {
            String x;

            if ((filter == null) || !filter.matcher(x = new String(buf, off, len)).matches()
                    || keep.matcher(x).matches()) {

                super.write(buf, 0, len);
            }
        }
    }
}
