/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.tools;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import edu.cshl.schatz.jnomics.io.JnomicsFileRecordReader;
import edu.cshl.schatz.jnomics.io.SequencingReadInputFormat;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsTool;
import edu.cshl.schatz.jnomics.mapreduce.ReadFileFormat;
import edu.cshl.schatz.jnomics.ob.writable.QueryTemplate;
import edu.cshl.schatz.jnomics.ob.writable.SequencingRead;

/**
 * This tool will attempt to determine the offset value used for the Phred score
 * encoding in a BAM or SAM file. It expects piped input.
 * 
 * @author Matthew Titmus
 */
public class PhredEncodingGuesser extends JnomicsTool {
    /**
     * Determines the minimum and maximum bounds of offset values used in a
     * Phred string.
     * 
     * @param phredString The Phred string.
     * @return An int[2] containing { minValue, maxValue }.
     * @throws RuntimeException If the string contains characters not in the
     *             range ('!' <= c <= '~').
     */
    public static int[] getOffsetBounds(Text phredString) {
        return getOffsetBounds(phredString, new int[] { Integer.MAX_VALUE, Integer.MIN_VALUE });
    }

    /**
     * Determines the minimum and maximum bounds of offset values used in a
     * Phred string, taking into account previous bounds. If
     * <code>phredString</code> does not contain values below the minimum or
     * maximum values contained in <code>currentBounds</code>, then
     * <code>currentBounds</code> is returned unchanged. Note that
     * <code>currentBounds</code> is directly modified by this method.
     * 
     * @param phredString The Phred string.
     * @param currentBounds Any initial bounds (from a previous call to this
     *            method).
     * @return An int[2] containing { minValue, maxValue }.
     * @throws RuntimeException If the string contains characters not in the
     *             range ('!' <= c <= '~').
     */
    public static int[] getOffsetBounds(Text phredString, int[] currentBounds) {
        int ascii;

        for (int i = 0; i < phredString.getLength(); i++) {
            ascii = phredString.charAt(i);

            if ((ascii < '!') || (ascii > '~')) {
                throw new RuntimeException("Not a Phred string: " + phredString);
            } else if (ascii < currentBounds[0]) {
                currentBounds[0] = ascii;
            } else if (ascii > currentBounds[1]) {
                currentBounds[1] = ascii;
            }
        }

        return currentBounds;
    }

    public static void main(String[] args) throws Exception {
        JnomicsTool.run(new PhredEncodingGuesser(), args);
    }

    @Override
    public Options buildDefaultJnomicsOptions(Options o, String... exclude) {
        exclude = new String[] { "out", "fout", "in" };

        return super.buildDefaultJnomicsOptions(o, exclude);
    }

    /*
     * @see
     * edu.cshl.schatz.jnomics.mapreduce.JnomicsTool#run(java.lang.String[])
     */
    @Override
    public int run(String[] args) throws Exception {
        int bounds[] = new int[] { Integer.MAX_VALUE, Integer.MIN_VALUE };
        int status = STATUS_ERROR_GENERAL;
        int readcount = 0;

        for (String arg : args) {
            SequencingReadInputFormat.addInputPath(getJob(), new Path(arg));
        }

        List<InputSplit> splits = new SequencingReadInputFormat().getSplits(getJob());
        FileSplit split = (FileSplit) splits.get(0);
        JnomicsFileRecordReader reader = SequencingReadInputFormat.newRecordReader(ReadFileFormat.get(split.getPath()));

        reader.initialize(split, getConf());

        while (reader.nextKeyValue()) {
            QueryTemplate qt = reader.getCurrentValue();

            for (SequencingRead read : qt) {
                bounds = getOffsetBounds(read.getPhredString(), bounds);
                readcount++;
            }
        }

        if (bounds[0] == Integer.MAX_VALUE) {
            System.err.println("Received no text from stdin");
        } else {
            System.out.printf(
                "Matches for identified phred "
                        + "range after %d reads: min=%d (\'%s\'), max=%d (\'%s\')%n%n", readcount,
                bounds[0], (char) bounds[0], bounds[1], (char) bounds[1]);

            // Grab encodings and sort by fitness value.
            Encoding[] encodings = Encoding.values();
            Arrays.sort(encodings, new FitnessComparator(bounds[0], bounds[1]));

            System.out.printf(
                "%-15s%-8s%3s/%-7s %s%n" + "-----------------------------------------------%n",
                "Description", "Offset", "Min", "Max", "Score");

            System.out.printf(
                "%-17s%-6s%3s/%-7s %s%n", "Your sample", "--", bounds[0], bounds[1], "--");

            for (int i = 0; i < encodings.length; i++) {
                Encoding e = encodings[i];
                String fitStr;

                if ((bounds[0] < e.minAscii) || (bounds[1] > e.maxAscii)) {
                    fitStr = "Out of bounds";
                } else {
                    double score = e.scoreEncodingFitScore(bounds[0], bounds[1]);
                    fitStr = String.format("%2.3f", score);

                    if (i == 0) {
                        fitStr += " (best fit)";
                    }
                }

                System.out.printf(
                    "%-17s%-6s%3s/%-7s %s%n", e.description, e.offset, e.minAscii, e.maxAscii,
                    fitStr);
            }
        }

        return status;
    }

    public static enum Encoding {
        /** Illumina 1.3+ Phred+64, raw reads typically (0, 40) */
        ILLUMINA_1_3("Illumina 1.3+", 64, 104, 64),

        /** Illumina 1.5+ Phred+64, raw reads typically (3, 40) */
        ILLUMINA_1_5("Illumina 1.5+", 66, 104, 64),

        /** Sanger Phred+33, raw reads typically (0, 40) */
        SANGER("Sanger", 33, 73, 33),

        /** Solexa Solexa+64, raw reads typically (-5, 40) */
        SOLEXA("Solexa", 59, 104, 64);

        public final String description;

        public final int minAscii, maxAscii, offset;

        Encoding(String description, int minAscii, int maxAscii, int offset) {
            this.description = description;
            this.minAscii = minAscii;
            this.maxAscii = maxAscii;
            this.offset = offset;
        }

        /**
         * A "fitness" score for given bounds. Range: (0..1] (1.0 = perfect fit)
         * 
         * @param lowerBound
         * @param upperBound
         */
        public double scoreEncodingFitScore(int lowerBound, int upperBound) {
            if ((upperBound > maxAscii) || (lowerBound < minAscii)) {
                return 0d;
            } else {
                double a = maxAscii - minAscii;
                double b = upperBound = lowerBound;

                return b / a;
            }
        }
    }

    private static class FitnessComparator implements Comparator<Encoding> {
        private int lowerBound, upperBound;

        /**
         * @param lowerBound
         * @param upperBound
         */
        FitnessComparator(int lowerBound, int upperBound) {
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
        }

        /*
         * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
         */
        public int compare(Encoding a, Encoding b) {
            double fitnessA = a.scoreEncodingFitScore(lowerBound, upperBound);
            double fitnessB = b.scoreEncodingFitScore(lowerBound, upperBound);
            int result = (int) (1000.0 * (fitnessB - fitnessA));

            return result;
        }
    }
}
