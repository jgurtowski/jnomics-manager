/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.test.util;

import java.io.File;
import java.io.IOException;

import edu.cshl.schatz.jnomics.ob.FastaFile;
import edu.cshl.schatz.jnomics.ob.FastaFileEntry;
import edu.cshl.schatz.jnomics.tools.SVSimulator;

/**
 * @author Matthew Titmus
 */
public class MakeTestCases {
    public static void main(String[] args) throws Exception {
        final String refSeq = "/home/mtitmus/Dev/data.genomes/Escherichia_coli_K12_100K/NC_000913.100k.fna";
        File cwd = new File("/home/mtitmus/sandbox/hydra-eval");
        File fastaFile = new File(refSeq);

        int[] sizeList = { 1000, 5000, 10000 };

        String name, fname;
        File outDir;
        int pos;

        FastaFileEntry fastaEntry = new FastaFile(fastaFile).get(0);
        SVSimulator simulator = new SVSimulator(fastaEntry);
        simulator.outputTo(new File(cwd, "original.fa"));

        name = "delete";
        for (int size : sizeList) {
            fname = name + "-" + size;
            outDir = new File(cwd, fname);
            System.out.println("Doing: " + fname);

            pos = (fastaEntry.length() - size) / 2;

            simulator = new SVSimulator(fastaEntry.clone());
            simulator.doDeletion(pos, size);
            doOutput(simulator, outDir, fname);
        }

        name = "inversion";
        for (int size : sizeList) {
            fname = name + "-" + size;
            outDir = new File(cwd, fname);
            System.out.println("Doing: " + fname);

            pos = (fastaEntry.length() - size) / 2;
            simulator = new SVSimulator(fastaEntry.clone());
            simulator.doInversion(pos, size);
            doOutput(simulator, outDir, fname);
        }

        name = "insertion";
        for (int size : sizeList) {
            fname = name + "-" + size;
            outDir = new File(cwd, fname);
            System.out.println("Doing: " + fname);

            pos = (fastaEntry.length() - size) / 2;
            simulator = new SVSimulator(fastaEntry.clone());
            simulator.doInsertion(pos, randomFasta(11000), 1, size);
            doOutput(simulator, outDir, fname);
        }

        name = "segmentalDuplication";
        for (int size : sizeList) {
            fname = name + "-" + size;
            outDir = new File(cwd, fname);
            System.out.println("Doing: " + fname);

            pos = fastaEntry.length() / 4;
            simulator = new SVSimulator(fastaEntry.clone());
            simulator.doSegmentalDuplication(pos, size, fastaEntry.length() - pos, false);
            doOutput(simulator, outDir, fname);
        }

        name = "segmentalDuplicationInverted";
        for (int size : sizeList) {
            fname = name + "-" + size;
            outDir = new File(cwd, fname);
            System.out.println("Doing: " + fname);

            pos = fastaEntry.length() / 4;
            simulator = new SVSimulator(fastaEntry.clone());
            simulator.doSegmentalDuplication(pos, size, fastaEntry.length() - pos, true);
            doOutput(simulator, outDir, fname);
        }

        name = "tandemDuplication";
        for (int size : sizeList) {
            fname = name + "-" + size;
            outDir = new File(cwd, fname);
            System.out.println("Doing: " + fname);

            pos = fastaEntry.length() / 2;
            simulator = new SVSimulator(fastaEntry.clone());
            simulator.doSegmentalDuplication(pos, size, pos, false);
            // simulator.doSegmentalDuplication(pos, size, pos, false);
            doOutput(simulator, outDir, fname);
        }
    }

    public static void main2(String[] args) throws Exception {
        final String refSeq = "/home/mtitmus/Dev/data.genomes/Escherichia_coli_K12_100K";
        File outDir = new File("/home/mtitmus/sandbox/hydra-eval/unified");
        File fastaFile = new File(refSeq);

        FastaFileEntry fastaEntry = new FastaFile(fastaFile).get(0);
        SVSimulator simulator = new SVSimulator(fastaEntry.clone());

        // Total length is ~4mb

        simulator.doDeletion(450000, 5000);
        simulator.doInversion(900000, 5000);
        simulator.doSegmentalDuplication(1350000, 5000, 1350000, false);
        simulator.doSegmentalDuplication(1800000, 5000, 1800000, false);
        simulator.doDeletion(1802500, 5000);

        simulator.doDeletion(2250000, 10000);
        simulator.doInversion(2700000, 10000);
        simulator.doSegmentalDuplication(3150000, 10000, 3150000, false);
        simulator.doSegmentalDuplication(3600000, 10000, 3600000, false);
        simulator.doDeletion(3605000, 10000);

        simulator.outputTo(new File(outDir, "multimutant.fa"));
        simulator.outputSummary(new File(outDir, "multimutant.sv"));
    }

    static void doOutput(SVSimulator simulator, File directory, String name) throws IOException {
        if (!directory.exists()) {
            directory.mkdir();
        }

        simulator.outputTo(new File(directory, name + ".fa"));
        simulator.outputSummary(new File(directory, name + ".sv"));
    }

    static FastaFileEntry randomFasta(int size) {
        final char[] BASES = { 'A', 'C', 'T', 'G' };

        String header = "";
        String identifier = "random bases";
        char[] bases = new char[size];

        int idx;
        for (int i = 0; i < size; i++) {
            idx = (int) (Math.random() * BASES.length);
            bases[i] = BASES[idx];
        }

        return new FastaFileEntry(header, identifier, new String(bases));
    }
}
