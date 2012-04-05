/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Collection;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import edu.cshl.schatz.jnomics.io.DNASequenceReader;
import edu.cshl.schatz.jnomics.ob.DNASequence;
import edu.cshl.schatz.jnomics.ob.FastaFile;

/**
 * For each method with a position parameter, the first base in the sequence is
 * at position 1.
 * 
 * @author Matthew Titmus
 */
public class SVSimulator {
    public static final String BUILD = "$Revision$";
    public static final String DATE = "$Date$";
    public static final String VERSION = "0.0.1";

    static final String HEADING = "SVSimulator v" + SVSimulator.VERSION + " build "
            + ", DNA Structural variation simulation tool.\n"
            + "2011, Matthew Titmus (mtitmus@cshl.edu)";

    static final PrintStream out = System.out, err = System.err;

    static final String USAGE = "Usage: svsim -r report-to [-i in.fa] [-o out.fa.sv]";

    private static final char[] BASES = new char[] { 'A', 'C', 'T', 'G' };

    /**
     * This value sets the upper limit of "short" variants at 50 (though the
     * odds of it reaching this size is about 100/(2^50) = 8.8e-14 percent)
     */
    private static final double MAX_VALUE = 1.125e15;

    private DNASequence mutantSequence = null;

    private Random random;

    private long randomSeed = System.currentTimeMillis();

    /**
	 * 
	 */
    public SVSimulator(DNASequence dnaSequence) throws IOException {
        random = new Random(randomSeed);
        mutantSequence = dnaSequence;
    }

    /**
	 * 
	 */
    public SVSimulator(File fastaFile) throws IOException {
        this(fastaFile, 0);
    }

    /**
     * @param entryIndex The position of the desired entry within the FASTA
     *            file. The first entry is 0, the second 1, and so on.
     */
    public SVSimulator(File fastaFile, int entryIndex) throws IOException {
        mutantSequence = new FastaFile(fastaFile).get(entryIndex);
    }

    public static void main(String[] args) throws Exception {
        final String referenceFastaFileName = "/home/mtitmus/Dropbox/Dev/data.genomes/Escherichia_coli_K12_100K/NC_000913.100k.fna";
        final String outputParentDirPath = "/home/mtitmus/sandbox/hydra-eval/";

        final String directoryName = "translocationUpstream-10000";
        final String mutantFileName = directoryName + ".sv.fa";
        final String summaryFileName = directoryName + ".summary";

        File outputDirectory = new File(outputParentDirPath, directoryName);
        File mutantFile = new File(outputDirectory, mutantFileName);
        File summaryFile = new File(outputDirectory, summaryFileName);

        SVSimulator sim = new SVSimulator(new File(referenceFastaFileName));
        DNASequence element;

        int firstIndex = 10001;
        int length = 10000;
        int insertBefore = 50001;
        boolean invert = false;
        element = new DNASequence(sim.getSequence(), firstIndex, length);

        element.reposition(1);
        if (invert) {
            element.invert(1, element.length());
        }

        sim.doDeletion(firstIndex, length);
        sim.getSequence().insert(insertBefore, element);

        outputDirectory.mkdir();
        sim.outputSummary(summaryFile);
        sim.outputTo(mutantFile);

        System.out.println("Done");

        // final int SV_SIZE = 2500;
        // int pos, div = 7500;
        //
        // pos = div;
        // int count = 0;
        // System.out.println((++count) + "\t" + pos + ".." + (pos + SV_SIZE) +
        // "\t" + "Deletion"
        // + "\t" + SV_SIZE + "bp");
        // sim.doDeletion(pos, SV_SIZE);
        //
        // pos += div;
        // System.out.println((++count) + "\t" + pos + ".." + (pos + SV_SIZE) +
        // "\t" + "Inversion"
        // + "\t" + SV_SIZE + "bp");
        // sim.doInversion(pos, SV_SIZE);
        //
        // pos += div;
        // System.out.println((++count) + "\t" + pos + ".." + (pos + SV_SIZE) +
        // "\t"
        // + "Tandem duplication" + "\t" + SV_SIZE + "bp");
        // sim.doSegmentalDuplication(pos, SV_SIZE, pos, false);
        //
        // pos += div;
        // System.out.println((++count) + "\t" + pos + ".." + (pos + SV_SIZE) +
        // "\t"
        // + "Tandem duplication (inverted)" + "\t" + SV_SIZE + "bp");
        // sim.doSegmentalDuplication(pos, SV_SIZE, pos, true);
        //
        // pos += div;
        // System.out.println((++count) + "\t" + (pos + div) + ".." + (pos + div
        // + SV_SIZE) + "\t"
        // + "Non-tandem duplication (upstream)" + "\t" + SV_SIZE + "bp from " +
        // (pos) + ".."
        // + (pos + SV_SIZE));
        // sim.doSegmentalDuplication(pos, SV_SIZE, pos + div, false);
        //
        // pos += (div * 2);
        // System.out.println((++count) + "\t" + (pos) + ".." + (pos + SV_SIZE)
        // + "\t"
        // + "Non-tandem duplication (downstream)" + "\t" + SV_SIZE + "bp from "
        // + (pos + div)
        // + ".." + (pos + div + SV_SIZE));
        // sim.doSegmentalDuplication(pos + div, SV_SIZE, pos, false);
        //
        // pos += div;
        // System.out.println((++count) + "\t" + (pos + div) + ".." + (pos + div
        // + SV_SIZE) + "\t"
        // + "Non-tandem duplication (upstream; inverted)" + "\t" + SV_SIZE +
        // "bp from "
        // + (pos) + ".." + (pos + SV_SIZE));
        // sim.doSegmentalDuplication(pos, SV_SIZE, pos + div, true);
        //
        // pos += (div * 2);
        // System.out.println((++count) + "\t" + (pos) + ".." + (pos + SV_SIZE)
        // + "\t"
        // + "Non-tandem duplication (downstream; inverted)" + "\t" + SV_SIZE +
        // "bp from "
        // + (pos + div) + ".." + (pos + div + SV_SIZE));
        // sim.doSegmentalDuplication(pos + div, SV_SIZE, pos, true);
        //
        // pos += div;
        // System.out.println((++count) + "\t" + (pos) + ".." + (pos + SV_SIZE)
        // + "\t"
        // + "Translocation (downstream; inverted)" + "\t" + SV_SIZE +
        // "bp from "
        // + (pos + div) + ".." + (pos + div + SV_SIZE));
        // sim.doSegmentalDuplication(pos + div, SV_SIZE, pos, true);
        // sim.doDeletion(pos, SV_SIZE);
    }

    public static void main2(String[] args) {
        Options options = getOptions();
        CommandLineParser parser = new GnuParser();
        InputStream fastaIn = System.in;
        PrintStream fastaOut = System.out, summaryOut = null;

        try {
            CommandLine cmd = parser.parse(options, args);
            SVSimulator svsim;

            if ((cmd.getOptions().length == 0) || cmd.hasOption('?')) {
                printHelp(out, options);
            } else if (cmd.getArgs().length > 0) {
                throw new ParseException("Unrecognized option: " + cmd.getArgs()[0]);
            } else {
                if (cmd.hasOption('i')) {
                    File fastaFile = new File(cmd.getOptionValue('i'));

                    try {
                        fastaIn = new FileInputStream(fastaFile);
                    } catch (FileNotFoundException e) {
                        throw new ParseException("Fasta file not found: "
                                + fastaFile.getAbsolutePath());
                    }
                }

                if (cmd.hasOption('o')) {
                    File fastaFile = new File(cmd.getOptionValue('o'));

                    try {
                        fastaOut = new PrintStream(fastaFile);
                    } catch (FileNotFoundException e) {
                        throw new ParseException("Cannot write to " + fastaFile + ": "
                                + e.getMessage());
                    }
                }

                if (cmd.hasOption('r')) {
                    File file = new File(cmd.getOptionValue('r'));

                    try {
                        summaryOut = new PrintStream(file);
                    } catch (FileNotFoundException e) {
                        throw new ParseException("Cannot write to " + file + ": " + e.getMessage());
                    }
                }

                svsim = new SVSimulator(new FastaFile(fastaIn).get(0));

                // svsim.doRandomVariants(2);
                svsim.outputTo(fastaOut);
                svsim.outputSummary(summaryOut);
            }
        } catch (ParseException pe) {
            err.println(pe.getMessage());
            printHelp(err, options);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if ((fastaIn != null) && (fastaIn instanceof FileInputStream)) {
                try {
                    fastaIn.close();
                } catch (IOException e) {}
            }
        }
    }

    private static Options getOptions() {
        Options options = new Options();
        Option o;

        options.addOption(new Option("i", "in", true, "The input FASTA file. Omit for stdin"));
        options.addOption(new Option("o", "out", true, "The output destination. Omit for stdout"));

        o = new Option("r", "report", true, "The destination to print the report. Required.");
        o.setRequired(true);
        options.addOption(o);

        options.addOption(new Option("?", "help", false, "Print this help message"));

        return options;
    }

    /**
	 * 
	 */
    @SuppressWarnings("unchecked")
    private static void printHelp(PrintStream ostream, Options options) {
        final String OPT_PATTERN = "  -%1$s, --%2$-15s %3$s%n";
        ostream.println(HEADING);
        ostream.println(USAGE);

        for (Option o : (Collection<Option>) options.getOptions()) {
            ostream.printf(OPT_PATTERN, o.getOpt(), o.getLongOpt(), o.getDescription());
        }
    }

    /**
	 * 
	 */
    public SVSimulator doDeletion() {
        int length = generateSizeValue(10000, 30000);
        int index = random.nextInt(mutantSequence.length() - length)
                + mutantSequence.getEndpoints().first();

        mutantSequence.delete(index, length);

        return this;
    }

    /**
     * @param firstIndex The index of the first base affected by this operation.
     */
    public SVSimulator doDeletion(int firstIndex, int length) {
        mutantSequence.delete(firstIndex, length);

        return this;
    }

    /**
	 * 
	 */
    public SVSimulator doDeletionSmall() {
        int length, firstIndex;

        do {
            length = generateSmallSizeValue();
        } while (mutantSequence.length() < length);

        firstIndex = random.nextInt(mutantSequence.length() - length)
                + mutantSequence.getEndpoints().first();
        mutantSequence.delete(firstIndex, length);

        return this;
    }

    public SVSimulator doInsertion(int insertBeforeIndex, DNASequence sequence) {
        return doInsertion(
            insertBeforeIndex, sequence, sequence.getEndpoints().first(), sequence.length());
    }

    public SVSimulator doInsertion(int insertBeforeIndex, DNASequence sequence,
        int sequenceFirstIndex, int sequenceInsertLength) {

        mutantSequence.insert(insertBeforeIndex, new DNASequence(
            sequence, sequenceFirstIndex, sequenceInsertLength));

        return this;
    }

    /**
     * Insert a small (generated by the <code>generateSmallSizeValue()</code> )
     * novel (random) sequence.
     */
    public SVSimulator doInsertionSmall() {
        DNASequence sequence;
        char bases[] = generateRandomBases(generateSmallSizeValue());
        int insertBeforeIndex;

        for (int i = 0; i < bases.length; i++) {
            bases[i] = BASES[random.nextInt(4)];
        }

        bases = generateRandomBases(generateSmallSizeValue());
        sequence = new DNASequence("-", bases);
        insertBeforeIndex = random.nextInt(mutantSequence.length())
                + mutantSequence.getEndpoints().first();

        return doInsertion(insertBeforeIndex, sequence);
    }

    /**
	 * 
	 */
    public SVSimulator doInversion() {
        int firstIndex, length = mutantSequence.length();

        while ((mutantSequence.length() - length) <= 0) {
            length = generateSizeValue(20000, 30000);
        }

        firstIndex = random.nextInt(mutantSequence.length() - length)
                + mutantSequence.getEndpoints().first();

        mutantSequence.invert(firstIndex, length);

        return this;
    }

    /**
     * @param firstIndex The index of the first base affected by this operation.
     */
    public SVSimulator doInversion(int firstIndex, int length) {
        mutantSequence.invert(firstIndex, length);

        return this;
    }

    /**
     * Introduces one or more random variants in the sequence.
     */
    public SVSimulator doRandomVariants(int varCount) {
        for (int i = 0; i < varCount; i++) {
            // TODO: Set relative ratios for each SV

            switch (random.nextInt(5)) {
            case 0:
                doInsertionSmall();
                break;
            case 1:
                doDeletionSmall();
                break;
            case 2:
                doDeletion();
                break;
            case 3:
                doSegmentalDuplication();
                break;
            case 4:
                doInversion();
                break;
            }
        }

        return this;
    }

    /**
	 * 
	 */
    public SVSimulator doSegmentalDuplication() {
        int length, firstIndex, insertPosition;

        do {
            length = generateSizeValue(10000, 10000);
        } while (mutantSequence.length() < length);

        firstIndex = random.nextInt(mutantSequence.length() - length)
                + mutantSequence.getEndpoints().first();
        insertPosition = random.nextInt(mutantSequence.length());

        return doSegmentalDuplication(firstIndex, length, insertPosition, random.nextBoolean());
    }

    public SVSimulator doSegmentalDuplication(int firstIndex, int length, int insertBeforeIndex,
        boolean invert) {

        mutantSequence.duplicate(firstIndex, length, insertBeforeIndex, invert);

        return this;
    }

    /**
	 * 
	 */
    public SVSimulator doTandemDuplication() {
        int length, firstIndex, insertPosition, duplicationCount;

        do {
            length = generateSizeValue(10000, 10000);
        } while (mutantSequence.length() < length);

        firstIndex = random.nextInt(mutantSequence.length() - length)
                + mutantSequence.getEndpoints().first();
        insertPosition = random.nextInt(mutantSequence.length());
        duplicationCount = 2 + random.nextInt(4) + random.nextInt(4) + random.nextInt(4)
                + random.nextInt(4) + random.nextInt(4) + random.nextInt(4);

        return doTandemDuplication(
            firstIndex, length, insertPosition, random.nextBoolean(), duplicationCount);
    }

    public SVSimulator doTandemDuplication(int firstIndex, int length, int insertBeforeIndex,
        boolean invert, int duplicationCount) {

        for (int i = 0; i < duplicationCount; i++) {
            mutantSequence.duplicate(firstIndex, length, insertBeforeIndex, invert);
        }

        return this;
    }

    /**
     * Generates a size value appropriate for a "small" variant, particularly a
     * small indel. The effective range is 1 to a very large number, but values
     * >6 occur less then 3% of the time, and >10 less than 0.2% of the time.
     */
    public int generateSmallSizeValue() {
        double value;

        value = random.nextDouble() * MAX_VALUE;
        value = 1 + Math.log((1 + MAX_VALUE) / (1 + value));

        return (int) value;
    }

    /**
     * @return The randomSeed
     */
    public long getRandomSeed() {
        return randomSeed;
    }

    /**
     * Returns the modified sequence.
     */
    public DNASequence getSequence() {
        return mutantSequence;
    }

    public void outputSummary(File file) throws IOException {
        outputSummary(new PrintStream(file));
    }

    public void outputSummary(PrintStream out) {
        mutantSequence.outputSummary(out);
    }

    public void outputTo(File file) throws IOException {
        outputTo(new PrintStream(file));
    }

    public void outputTo(PrintStream out) throws IOException {
        DNASequenceReader in = mutantSequence.getSequenceReader();
        String line, header;

        if ((header = mutantSequence.getReferenceName()).length() == 0) {
            header = "UnknownSequence-"
                    + Integer.toHexString(mutantSequence.hashCode()).toUpperCase();
        }

        out.println(">" + header);
        while (null != (line = in.readLine())) {
            out.println(line);
        }
    }

    /**
     * @param randomSeed The randomSeed to set
     */
    public void setRandomSeed(long randomSeed) {
        this.randomSeed = randomSeed;
        random = new Random(randomSeed);
    }

    private char[] generateRandomBases(int length) {
        char bases[] = new char[length];

        for (int i = 0; i < bases.length; i++) {
            bases[i] = BASES[random.nextInt(4)];
        }

        return bases;
    }

    private int generateSizeValue(double mu, double sigma) {
        return generateSizeValue(mu, sigma, 1, (int) 10e10);
    }

    /**
     * Generates a size value from the supplied constraints.
     */
    private int generateSizeValue(double mu, double sigma, int minSize, int maxSize) {

        int size = 0;

        // If minSize > maxSize, assume swap them.
        if (minSize > maxSize) {
            int tmp = maxSize;
            maxSize = minSize;
            minSize = tmp;
        }

        if (maxSize < (mu - (2 * sigma))) {
            System.out.println("Warning: maximum variation size < (mu - (2 x sigma)). Setting size to maximum size.");
            size = maxSize;
        } else if (minSize > (mu + (2 * sigma))) {
            System.out.println("Warning: minimum variation size > (mu + (2 x sigma)). Setting size to minimum size.");
            size = minSize;
        } else {
            while ((size < minSize) || (size > maxSize)) {
                size = (int) ((random.nextGaussian() * sigma) + mu);
            }
        }

        return size;
    }
}
