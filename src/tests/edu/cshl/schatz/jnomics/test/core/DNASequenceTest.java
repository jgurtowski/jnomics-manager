/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.test.core;

import java.io.IOException;

import edu.cshl.schatz.jnomics.ob.DNASequence;
import edu.cshl.schatz.jnomics.ob.Orientation;

@SuppressWarnings("all")
/**
 * TODO Convert this into a JUnit test
 * @author Matthew A. Titmus
 */
class DNASequenceTest {
    static void testBreaks() {
        DNASequence seq;
        int index = 0;
        final String HEADER_FORMAT = "%n%n=================================%n" + "%1$s: %n"
                + "Return value: %2$s%n" + "%3$s%n---------------------------------%n"
                + "Idx\tFrom\tTo\tName%n---------------------------------%n";

        seq = new DNASequence("TEST", "AAAACCCCTTTTGGGGUUUU");

        // Initial conditions
        // Expected sequence: "AAAACCCCTTTTGGGGUUUU"

        // index = 0;
        // System.out.printf(
        // HEADER_FORMAT,
        // "Initial conditions",
        // "",
        // seq.toString());
        // for (SequenceSegment segment : seq.segments) {
        // System.out.printf("%1$2s\t%2$s%n", index++, segment.toString());
        // }

        // Test: createBreakpoint call at existing breakpoint
        // Expected return: [0, 0]
        // Expected segments: No change
        // Expected sequence: "AAAACCCCTTTTGGGGUUUU"

        // System.out.printf(
        // HEADER_FORMAT,
        // "createBreakpoint(1)",
        // Arrays.toString(seq.createBreakpoint(1)),
        // seq.toString());
        // index = 0;
        // for (SequenceSegment segment : seq.get segments) {
        // System.out.printf(
        // "%1$s\t%2$s\t%3$s%n",
        // index++,
        // segment.toString(),
        // segment.getSequenceString());
        // }

        // Test: createBreakpoint(8)
        // Expected return: [0, 1]
        // Expected segments: "AAAACCCC", "TTTTGGGGUUUU"
        // Expected sequence: "AAAACCCCTTTTGGGGUUUU"

        // System.out.printf(
        // HEADER_FORMAT,
        // "createBreakpoint(8)",
        // Arrays.toString(seq.createBreakpoint(8)),
        // seq.toString());
        // index = 0;
        // for (SequenceSegment segment : seq.segments) {
        // System.out.printf(
        // "%1$s\t%2$s\t%3$s%n",
        // index++,
        // segment.toString(),
        // segment.getSequenceString());
        // }
    }

    static void testDelete() throws IOException {
        // Initial conditions
        // Expected sequence: "AAAACCCCTTTTGGGGUUUU"
        DNASequence seq = new DNASequence("TEST", "AAAACCCCTTTTGGGGUUUU");
        System.out.println("Initial: " + seq.toString());

        // Test: doDelete(1,0)
        // Expected sequence: "AAAACCCCTTTTGGGGUUUU"
        seq.delete(1, 0);
        System.out.println("0,0: " + seq.toString());
        seq.outputSummary(System.out);

        // Test: doDelete(10,0)
        // Expected sequence: "AAAACCCCTTTTGGGGUUUU"
        seq.delete(10, 0);
        System.out.println("10,0: " + seq.toString());
        seq.outputSummary(System.out);

        // Test: doDelete(20,0)
        // Expected sequence: "AAAACCCCTTTTGGGGUUUU"
        seq.delete(20, 0);
        System.out.println("20,0: " + seq.toString());
        seq.outputSummary(System.out);

        // Test: doDelete(1,4)
        // Expected sequence: "CCCCTTTTGGGGUUUU"
        seq.delete(1, 4);
        System.out.println("0,4: " + seq.toString());
        seq.outputSummary(System.out);

        // Test: doDelete(4,4)
        // Expected sequence: "CCCCGGGGUUUU"
        seq.delete(4, 4);
        System.out.println("4,4: " + seq.toString());
        seq.outputSummary(System.out);

        // Test: doDelete(8,4)
        // Expected sequence: "CCCCGGGG"
        seq.delete(8, 4);
        System.out.println("8,4: " + seq.toString());
        seq.outputSummary(System.out);

        // Test: doDelete(1,8)
        // Expected sequence: ""
        seq.delete(1, 8);
        System.out.println("0,8: " + seq.toString());
        seq.outputSummary(System.out);
    }

    static void testDuplicateInverted() throws IOException {
        DNASequence seq, ins;

        // Expected sequence: "AAAACCCCTTTTGGGGUUUU"
        seq = new DNASequence("Base", "");
        seq.add(new DNASequence("Original", "AAAACCCCUUUU"));
        seq.insert(8, new DNASequence("inter-TTTT", "TTTTGGGG"));
        System.out.println(seq.toString());
        seq.outputSummary(System.out);

        // Expected sequence: "AAAACCCCTTTTGGGGUUUU"
        seq.duplicate(1, 0, true);
        System.out.println(seq.toString());
        seq.outputSummary(System.out);

        // Expected sequence: "AAAACCCCGGGGTTTTTTTTGGGGUUUU"
        seq.duplicate(1, 8, true);
        System.out.println(seq.toString());
        seq.outputSummary(System.out);

        // Expected sequence: "AAAACCCCGGGGTTTTTTTTAAAAAAAAGGGGUUUU"
        seq.duplicate(12, 8, true);
        System.out.println(seq.toString());
        seq.outputSummary(System.out);
    }

    static void testDuplicateNormal() throws IOException {
        DNASequence seq, ins;

        // Expected sequence: "AAAACCCCTTTTGGGGUUUU"
        seq = new DNASequence("Base", "");
        seq.add(new DNASequence("Original", "AAAACCCCUUUU"));
        seq.insert(8, new DNASequence("inter-TTTT", "TTTTGGGG"));
        System.out.println(seq.toString());
        seq.outputSummary(System.out);

        // Expected sequence: "AAAACCCCTTTTGGGGUUUU"
        seq.duplicate(1, 0, false);
        System.out.println(seq.toString());
        seq.outputSummary(System.out);

        // Expected sequence: "AAAACCCCAAAACCCCTTTTGGGGUUUU"
        seq.duplicate(1, 8, false);
        System.out.println(seq.toString());
        seq.outputSummary(System.out);

        // Expected sequence: "AAAACCCCAAAACCCCTTTTCCCCTTTTGGGGUUUU"
        seq.duplicate(12, 8, false);
        System.out.println(seq.toString());
        seq.outputSummary(System.out);
    }

    static void testEntropy() {
        DNASequence seq;
        String bases, pattern = "%1$s: Got %2$s; expected %3$s%n";
        double expect;

        bases = "AAAA";
        expect = 0.0;
        System.out.printf(pattern, bases, (seq = new DNASequence("", bases)).getEntropy(), expect);

        bases = "AAGG";
        expect = 1.0;
        System.out.printf(pattern, bases, (seq = new DNASequence("", bases)).getEntropy(), expect);

        bases = "GGGC";
        expect = .81;
        System.out.printf(pattern, bases, (seq = new DNASequence("", bases)).getEntropy(), expect);

        bases = "GGSS";
        expect = .81;
        System.out.printf(pattern, bases, (seq = new DNASequence("", bases)).getEntropy(), expect);

        bases = "AAWW";
        expect = .81;
        System.out.printf(pattern, bases, (seq = new DNASequence("", bases)).getEntropy(), expect);
    }

    static void testGCCalculations() {
        String dna[] = {
                "AAAAACCCCCTTTTTGGGGG",
                "AAAAAAAAAAAAAAAAAAAA",
                "AAAAAAAAAACCCCCCCCCC",
                "AAAAAAAAAATTTTTTTTTT",
                "AAAAAAAAAAGGGGGGGGGG",
                "GGGGGGGGGGAAAAAAAAAA",
                "GGGGGGGGGGCCCCCCCCCC",
                "GGGGGGGGGGTTTTTTTTTT",
                "GGGGGGGGGGGGGGGGGGGG", };
        DNASequence seq;

        for (String s : dna) {
            seq = new DNASequence(s, s);
            System.out.println(s + ": " + seq.getGCContent());
        }
    }

    static void testInsert() throws IOException {
        DNASequence seq, ins;

        // Test: insert(1, "TTTT")
        // Expected sequence: "TTTTAAAAAAAAAAAAAAAAAAAA"
        seq = new DNASequence("Original", "AAAAAAAAAAAAAAAAAAAA");
        ins = new DNASequence("5'-TTTT", "TTTT", 10, Orientation.MINUS);
        seq.insert(1, ins);
        System.out.println(seq.toString());
        seq.outputSummary(System.out);

        // Test: insert("TTTT")
        // Expected sequence: "AAAAAAAAAAAAAAAAAAAATTTT"
        seq = new DNASequence("Original", "AAAAAAAAAAAAAAAAAAAA");
        ins = new DNASequence("3'-TTTT", "TTTT");
        seq.add(ins);
        System.out.println(seq.toString());
        seq.outputSummary(System.out);
        System.out.println();

        // Test: insert(10, "TTTT")
        // Expected sequence: "AAAAAAAAAATTTTAAAAAAAAAA"
        seq = new DNASequence("Original", "AAAAAAAAAAAAAAAAAAAA");
        ins = new DNASequence("inter-TTTT", "TTTT");
        seq.insert(10, ins);
        System.out.println(seq.toString());
        seq.outputSummary(System.out);

        String[] toAdd = new String[] { "CCCC", "TTTT", "UUUU" };
        seq = new DNASequence("Multi-add", "AAAAAAAAAAAAAAAAAAAA");
        System.out.println(seq);
        for (String s : toAdd) {
            seq.insert(10, new DNASequence(s, s, 10, Orientation.MINUS));
            System.out.println(seq);
            seq.outputSummary(System.out);
        }

        System.out.println(seq);
        seq.outputSummary(System.out);
    }

    static void testInvert() throws IOException {
        // Initial conditions
        // Expected sequence: "AAAACCCCTTTTGGGGUUUU"
        DNASequence seq;
        seq = new DNASequence("Sequence", "");
        seq.add(new DNASequence("Part1", "AAAACCCCTT"));
        seq.add(new DNASequence("Part2", "TTGGGGUUUU", 100, Orientation.MINUS));
        System.out.println(seq.toString());
        seq.outputSummary(System.out);
        System.out.println();

        // Test: doInvert(1,0)
        // Expected sequence: "AAAACCCCTTTTGGGGUUUU"
        seq.invert(1, 0);
        System.out.println(seq.toString());
        seq.outputSummary(System.out);
        System.out.println();

        // Test: doInvert(10,0)
        // Expected sequence: "AAAACCCCTTTTGGGGUUUU"
        seq.invert(10, 0);
        System.out.println(seq.toString());
        seq.outputSummary(System.out);
        System.out.println();

        // Test: doInvert(20,0)
        // Expected sequence: "AAAACCCCTTTTGGGGUUUU"
        seq.invert(20, 0);
        System.out.println(seq.toString());
        seq.outputSummary(System.out);
        System.out.println();

        // Test: doInvert(1,8)
        // Expected sequence: "GGGGTTTTTTTTGGGGUUUU"
        seq = new DNASequence("Sequence2", "");
        seq.add(new DNASequence("Part1", "AAAACCCCTT"));
        seq.add(new DNASequence("Part2", "TTGGGGUUUU", 100, Orientation.MINUS));
        seq.invert(1, 8);
        System.out.println(seq.toString());
        seq.outputSummary(System.out);
        System.out.println();

        // Test: doInvert(1,20)
        // Expected sequence: "AAAACCCCAAAAAAAACCCC"
        seq.invert(1, 20);
        System.out.println(seq.toString());
        seq.outputSummary(System.out);
        System.out.println();

        // Test: doInvert(12,8)
        // Expected sequence: "AAAACCCCAAAAGGGGTTTT"
        seq.invert(12, 8);
        System.out.println(seq.toString());
        seq.outputSummary(System.out);
        System.out.println();

        // Test: doInvert(4,1)
        // Expected sequence: "AAAAGCCCAAAAGGGGTTTT"
        seq.invert(4, 1);
        System.out.println(seq.toString());
        seq.outputSummary(System.out);
        System.out.println();

        // Test: doInvert(4,4)
        // Expected sequence: "AAAAGGGCAAAAGGGGTTTT"
        seq.invert(4, 4);
        System.out.println(seq.toString());
        seq.outputSummary(System.out);
        System.out.println();

        // Test: doInvert(4,4)
        // Expected sequence: "AAAACCCCAAAAGGGGTTTT"
        seq.invert(4, 3);
        System.out.println(seq.toString());
        seq.outputSummary(System.out);
        System.out.println();

        // Test: doInvert(4,4)
        // Expected sequence: "AAAACCCCTTTTGGGGTTTT"
        seq.invert(1, 20);
        System.out.println(seq.toString());
        seq.outputSummary(System.out);
        System.out.println();
    }
}