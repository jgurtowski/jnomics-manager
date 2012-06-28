/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.test.tools;


/**
 * Temporarily disabled until the DNASequenceInputStream class can be
 * refactored.
 * 
 * @author Matthew A. Titmus
 */
public class SVSimTest {
    // extends TestCase {

    // private String initialSeq, finalSeq, otherSeq;
    // private SVSimulator svsim;
    //
    // private static final SequenceOpts seq(String sequence) {
    // return seq(sequence, "name");
    // }
    //
    // private static final SequenceOpts seq(String sequence, String
    // sequenceName) {
    // return new SequenceOpts(sequenceName, sequence);
    // }
    //
    // private void doCompare(SequenceOpts mutant, String expectedAsString) {
    // String mutantAsString = mutant.getSequenceString();
    //
    // assertEquals(expectedAsString, mutantAsString);
    // assertEquals(expectedAsString.length(), mutant.length());
    // }
    //
    // public void testDeletion_Size0() throws Exception {
    // finalSeq = initialSeq = "GATTACA";
    // svsim = new SVSimulator(seq(initialSeq)).doDeletion(5, 0);
    //
    // doCompare(svsim.getSequence(), finalSeq);
    // }
    //
    // public void testDeletion_Size3() throws Exception {
    // initialSeq = "GATTACA";
    // finalSeq = "TACA";
    //
    // svsim = new SVSimulator(seq(initialSeq)).doDeletion(1, 3);
    //
    // doCompare(svsim.getSequence(), finalSeq);
    // }
    //
    // public void testDeletion_SizeAll() throws Exception {
    // initialSeq = "GATTACA";
    // finalSeq = "";
    //
    // svsim = new SVSimulator(seq(initialSeq)).doDeletion(1,
    // initialSeq.length());
    //
    // doCompare(svsim.getSequence(), finalSeq);
    // }
    //
    // public void testInsertion_Size0() throws Exception {
    // finalSeq = initialSeq = "AAAAATTTTT";
    // otherSeq = "";
    //
    // svsim = new SVSimulator(seq(initialSeq)).doInsertion(5, seq(otherSeq,
    // "other"));
    //
    // doCompare(svsim.getSequence(), finalSeq);
    // }
    //
    // public void testInsertion_SizeLast() throws Exception {
    // initialSeq = "AAAA";
    // otherSeq = "GGG";
    // finalSeq = initialSeq + otherSeq;
    //
    // SequenceOpts iSeq = seq(initialSeq);
    // SequenceOpts oSeq = seq(otherSeq, "other");
    //
    // svsim = new SVSimulator(iSeq);
    // svsim.doInsertion(5, oSeq);
    //
    // doCompare(svsim.getSequence(), finalSeq);
    // }
    //
    // public void testInsertion_SizeMiddle() throws Exception {
    // initialSeq = "AAAAATTTTT";
    // otherSeq = "GGG";
    // finalSeq = "AAAAA" + otherSeq + "TTTTT";
    //
    // svsim = new SVSimulator(seq(initialSeq)).doInsertion(6, seq(otherSeq,
    // "other"));
    //
    // doCompare(svsim.getSequence(), finalSeq);
    // }
    //
    // public void testInsertion_SizeStart() throws Exception {
    // initialSeq = "AAAA";
    // otherSeq = "GGG";
    // finalSeq = otherSeq + initialSeq;
    //
    // svsim = new SVSimulator(seq(initialSeq)).doInsertion(1, seq(otherSeq,
    // "other"));
    //
    // doCompare(svsim.getSequence(), finalSeq);
    // }
    //
    // public void testInversion_All() throws Exception {
    // initialSeq = "CCCTTTTT";
    // finalSeq = "AAAAAGGG";
    //
    // svsim = new SVSimulator(seq(initialSeq)).doInversion(1,
    // initialSeq.length());
    //
    // doCompare(svsim.getSequence(), finalSeq);
    // }
    //
    // public void testInversion_SeveralSegments() throws Exception {
    // initialSeq = "AAATTTTGGCCCCC";
    // String finalSeq1 = "AAATTGCCAACCCC";
    // String finalSeq2 = "GGGGTTGGCAATTT";
    //
    // svsim = new SVSimulator(seq("AAA"));
    // svsim = svsim.doInsertion(1 + svsim.getSequence().length(), seq("TTTT"));
    // svsim = svsim.doInsertion(1 + svsim.getSequence().length(), seq("GG"));
    // svsim = svsim.doInsertion(1 + svsim.getSequence().length(),
    // seq("CCCCC"));
    // doCompare(svsim.getSequence(), initialSeq);
    //
    // svsim = svsim.doInversion(6, 5);
    // doCompare(svsim.getSequence(), finalSeq1);
    //
    // svsim = svsim.doInversion(1, svsim.getSequence().length());
    // doCompare(svsim.getSequence(), finalSeq2);
    // }
    //
    // public void testInversion_Size0() throws Exception {
    // initialSeq = "CCCTTTTT";
    // finalSeq = "CCCTTTTT";
    //
    // svsim = new SVSimulator(seq(initialSeq)).doInversion(3, 0);
    //
    // doCompare(svsim.getSequence(), finalSeq);
    // }
    //
    // public void testInversion_Size1() throws Exception {
    // initialSeq = "CCCTTTTT";
    // finalSeq = "CCGTTTTT";
    //
    // svsim = new SVSimulator(seq(initialSeq)).doInversion(3, 1);
    //
    // doCompare(svsim.getSequence(), finalSeq);
    // }
    //
    // public void testInversion_Size2() throws Exception {
    // initialSeq = "CCCTTTTT";
    // finalSeq = "CCAGTTTT";
    //
    // svsim = new SVSimulator(seq(initialSeq)).doInversion(3, 2);
    //
    // doCompare(svsim.getSequence(), finalSeq);
    // }
}
