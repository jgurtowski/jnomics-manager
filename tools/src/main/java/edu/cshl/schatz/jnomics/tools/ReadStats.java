/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.tools;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;

import edu.cshl.schatz.jnomics.mapreduce.JnomicsTool;
import edu.cshl.schatz.jnomics.ob.writable.QueryTemplate;
import edu.cshl.schatz.jnomics.ob.writable.SequencingRead;

/**
 * <p>
 * </p>
 * 
 * @author Matthew Titmus
 */
public class ReadStats extends JnomicsTool {
    public static final String CMD_DESCRIPTION = "Read alignment flags and insert size and statistics";

    public static final String CMD_HEADING = CMD_DESCRIPTION + "\n"
            + "2011, Matthew Titmus (mtitmus@cshl.edu)";

    public static final String CMD_NAME = "stats";

    public static final String CMD_USAGE = CMD_NAME
            + " [options] -in <file> -out <path> -fin <format>";

    public static final int DEFAULT_BUCKET_SIZE = 5;

    public static final String P_BUCKET_SIZE = "jnomics.stats.bucket.size";

    static final Log LOG = LogFactory.getLog(ReadStats.class);

    static final String NAME = ReadStats.class.getSimpleName();

    private static final int MAX_VALUE = 10000;

    private static final int QC_FAIL = 1, QC_PASS = 0;

    public ReadStats() {
        setHelpUsage(CMD_USAGE);
        setHelpHeading(CMD_HEADING);
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new ReadStats(), args);
    }

    /*
     * @see
     * edu.cshl.schatz.jnomics.mapreduce.JnomicsTool#handleOptions(org.apache
     * .commons.cli.CommandLine)
     */
    @Override
    public int handleOptions(CommandLine cmd) throws ParseException, IOException {
        return STATUS_OK;
    }

    /*
     * @see
     * edu.cshl.schatz.jnomics.mapreduce.JnomicsTool#run(java.lang.String[])
     */
    @Override
    public int run(String[] args) throws Exception {
        getJob().setJarByClass(ReadStats.class);
        getJob().setReducerClass(ReadStatsReducer.class);

        return getJob().waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Counts the number of times each read flag value is seen.
     */
    static class FlagStatsReader {
        // 100000 + 0 in total (QC-passed reads + QC-failed reads)
        // 0 + 0 duplicates
        // 99516 + 0 mapped (99.52%:-nan%)
        // 100000 + 0 paired in sequencing
        // 50000 + 0 read1
        // 50000 + 0 read2
        // 95130 + 0 properly paired (95.13%:-nan%)
        // 99228 + 0 with itself and mate mapped
        // 288 + 0 singletons (0.29%:-nan%)
        // 0 + 0 with mate mapped to a different chr
        // 0 + 0 with mate mapped to a different chr (mapQ>=5)

        int[] duplicates = new int[2];
        int[] mapped = new int[2];
        int[] mateMappedToDiffChr = new int[2];
        int[] mateMappedToDiffChrMapqGT5 = new int[2];
        int[] pairedInSequencing = new int[2];
        int[] properlyPaired = new int[2];
        int[] read1 = new int[2];
        int[] read2 = new int[2];
        int[] readNot1or2 = new int[2];
        int[] selfAndMateMapped = new int[2];
        int[] singletons = new int[2];
        int[] totals = new int[2];

        public void stat(QueryTemplate template) {
            for (SequencingRead read : template) {
                stat(read);
            }
        }

        public void stat(SequencingRead read) {
            QueryTemplate template = read.getQueryTemplate();
            int qc = read.isFailedQuality() ? QC_FAIL : QC_PASS;

            totals[qc]++;

            if (read.isMapped()) {
                mapped[qc]++;

                if (read.isProperlyPaired()) {
                    properlyPaired[qc]++;
                }

                if (read.isMapped() && read.isNextMapped()) {
                    selfAndMateMapped[qc]++;
                }
            }

            if (read.isDuplicate()) {
                duplicates[qc]++;
            }

            if (read.isTemplateMultiplySegmented()) {
                pairedInSequencing[qc]++;

                if (read.isFirst() && read.isLast()) {
                    readNot1or2[qc]++;
                } else {
                    SequencingRead mate = null;

                    if (read.isFirst()) {
                        read1[qc]++;
                        mate = template.getLast();
                    } else if (read.isLast()) {
                        read2[qc]++;
                        mate = template.getFirst();
                    }

                    if ((mate != null)
                            && !read.getReferenceNameText().equals(mate.getReferenceNameText())) {
                        mateMappedToDiffChr[qc]++;

                        if ((read.getMappingQuality() >= 5) && (mate.getMappingQuality() >= 5)) {
                            mateMappedToDiffChrMapqGT5[qc]++;
                        }
                    }
                }
            } else {
                // Is "singleton" the same as template.size() == 1?
                singletons[qc]++;
            }
        }
    }

    static class ReadStatsReducer extends Reducer<Text, QueryTemplate, Text, QueryTemplate> {
        private int aboveMax = 0;

        private int[] bucketContents;

        private int bucketSize = DEFAULT_BUCKET_SIZE;

        private long insertSizeSum = 0;

        private FlagStatsReader stats = new FlagStatsReader();

        /*
         * @see
         * org.apache.hadoop.mapreduce.Reducer#cleanup(org.apache.hadoop.mapreduce
         * .Reducer.Context)
         */
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            context.getCounter(NAME, "In total (QC-passed)").increment(stats.totals[QC_PASS]);
            context.getCounter(NAME, "Duplicates (QC-passed)").increment(stats.duplicates[QC_PASS]);
            context.getCounter(NAME, "Mapped (QC-passed)").increment(stats.mapped[QC_PASS]);
            context.getCounter(NAME, "Paired in sequencing (QC-passed)").increment(
                stats.pairedInSequencing[QC_PASS]);
            context.getCounter(NAME, "Read1 (QC-passed)").increment(stats.read1[QC_PASS]);
            context.getCounter(NAME, "Read2 (QC-passed)").increment(stats.read2[QC_PASS]);
            context.getCounter(NAME, "ReadN (not 1 or 2) (QC-passed)").increment(
                stats.readNot1or2[QC_PASS]);
            context.getCounter(NAME, "Properly paired (QC-passed)").increment(
                stats.properlyPaired[QC_PASS]);
            context.getCounter(NAME, "With itself and mate mapped (QC-passed)").increment(
                stats.selfAndMateMapped[QC_PASS]);
            context.getCounter(NAME, "Singletons (QC-passed)").increment(stats.singletons[QC_PASS]);
            context.getCounter(NAME, "With mate mapped to a different chr").increment(
                stats.mateMappedToDiffChr[QC_PASS]);
            context.getCounter(NAME, "With mate mapped to a different chr (mapQ>=5)").increment(
                stats.mateMappedToDiffChrMapqGT5[QC_PASS]);

            // TODO Write this to an output file
            long average = insertSizeSum / stats.totals[QC_PASS];

            System.out.println("# Insert size average: " + average);
            // TODO System.out.println("# Insert size stddev: " + average);

            // Find the indices of the first and last buckets that have any
            // contents.

            int firstIndex = 0, lastIndex = 0, i;

            for (i = 0; (firstIndex == 0) && (i < bucketContents.length); i++) {
                if (bucketContents[i] > 0) {
                    firstIndex = i;
                }
            }

            for (i = bucketContents.length - 1; (lastIndex == 0) && (i >= firstIndex); i--) {
                if (bucketContents[i] > 0) {
                    lastIndex = i;
                }
            }

            // "bucketStart" is the smallest value in the bucket's range.

            IntWritable bucketStartWriteable = new IntWritable();
            IntWritable bucketContentsWriteable = new IntWritable();

            for (i = firstIndex; i <= lastIndex; i++) {
                bucketStartWriteable.set(i * bucketSize);
                bucketContentsWriteable.set(bucketContents[i]);

                // TODO
                // context.write(bucketStartWriteable, bucketContentsWriteable);
            }
        }

        @Override
        public void reduce(Text key, Iterable<QueryTemplate> values, Context context)
                throws IOException, InterruptedException {

            for (QueryTemplate value : values) {
                stats.stat(value);
            }
        }

        /*
         * @see
         * org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce
         * .Reducer.Context)
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            bucketSize = context.getConfiguration().getInt(P_BUCKET_SIZE, 5);

            // Set a large bucket size.
            bucketContents = new int[2048 / bucketSize];
        }

        private void countReadLength(int value) {
            insertSizeSum = value;

            int bucketIndex = value / bucketSize;

            if (value > MAX_VALUE) {
                aboveMax++;
            } else {
                try {
                    bucketContents[bucketIndex]++;
                } catch (ArrayIndexOutOfBoundsException e) {
                    // If we got here, then the array needs to be resized.
                    // Interestingly, when the number of tests is very large
                    // it's actually faster to catch a rare exception then it is
                    // check against the array size many times.

                    // The new number of buckets is the requested value * 1.25.
                    int[] newBucketContents = new int[bucketIndex + (bucketIndex >> 2)];

                    LOG.info("Resizing buckets array: " + bucketContents.length + " --> "
                            + newBucketContents.length);

                    System.arraycopy(bucketContents, 0, newBucketContents, 0, bucketContents.length);

                    bucketContents = newBucketContents;

                    countReadLength(value);
                }
            }
        }
    }
}
