/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.test.core;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.nio.charset.CharacterCodingException;

import junit.framework.TestCase;

import org.apache.hadoop.io.Text;

import edu.cshl.schatz.jnomics.ob.Orientation;
import edu.cshl.schatz.jnomics.ob.PositionRange;
import edu.cshl.schatz.jnomics.ob.writable.QueryTemplate;
import edu.cshl.schatz.jnomics.ob.writable.SequencingRead;
import edu.cshl.schatz.jnomics.test.util.IdentityTester;
import edu.cshl.schatz.jnomics.test.util.IdentityTester.AssertThat;
import edu.cshl.schatz.jnomics.util.TextCutter;

/**
 * @author Matthew Titmus
 */
public class WritablesTest extends TestCase {
    private static final String CIGAR1 = "81M17S", CIGAR2 = "101M";
    private static final int FLAGS1 = 99, FLAGS2 = 147;

    private static final int MAPPOS1 = 61689, MAPPOS2 = 61959;
    private static final int MAPQ1 = 22, MAPQ2 = 23;
    private static final int NEXTP1 = MAPPOS2, NEXTP2 = MAPPOS1;
    private static final String PHRED1 = "FFFFBBDBDBDDB8FGGDGB?;=:?7=?<?EGGD>CE@CCGGGDG8GGDGGEGB@GGGE>DDGG@GBDD8;84:4@C>@C##################";
    private static final String PHRED2 = "<HBEGIEBFCC@DCIBDFFBBHE@IFD<HCFEIIFG@GII>GBDGEE>DGDHBBIHIHIIIIIGIHIHHIIIIIIIIIIIIIIIDIIIIIIIHIIIIIIII";
    private static final String QNAME = "BILLIEHOLIDAY_0011:1:101:3558:14195#0";
    private static final String REFERENCE = "chr10";
    private static final String REFNEXT1 = "=", REFNEXT2 = "=";

    private static final String SEQ1 = "CTTTGTGTATAGCAGGGAATCTGTGTCTAATTTGTAGTATTTCATGCTTCTAGGTTTTCATGGCAGTTGAGATGTAAGAATAACAATAGTGTTGGGAG";
    private static final String SEQ2 = "AAGATGGAAGTCTGGGCCTACCTCCTCTCTTTTATTAATATGTAGACAGGACACCAACACAAATTAGAATGAAGACAAACAAAATGTTAGCAAATGAAGAA";

    private static final String TAGS1 = "XC:i:81\tXT:A:R\tNM:i:0\tSM:i:0\tAM:i:0\tX0:i:2\tX1:i:0\tXM:i:0\tXO:i:0\tXG:i:0\tMD:Z:81";
    private static final String TAGS2 = "XT:A:U\tNM:i:0\tSM:i:23\tAM:i:0\tX0:i:1\tX1:i:1\tXM:i:0\tXO:i:0\tXG:i:0\tMD:Z:101";

    private static final int TEMPLATE_LENGTH = 371;

    private QueryTemplate queryTemplate = new QueryTemplate();

    private SequencingRead read1 = new SequencingRead(), read2 = new SequencingRead();

    public WritablesTest() throws CharacterCodingException {
        _reset();
    }

    public static void assertEquals(String expected, Text actual) {
        assertEquals(expected, actual.toString());
    }

    public static void assertEquals(Text expected, Text actual) {
        assertEquals(expected.toString(), actual.toString());
    }

    public void _reset() {
        TextCutter tabCutter = new TextCutter().setDelimiter('\t');
        TextCutter colonCutter = new TextCutter().setDelimiter(':');
        Text key, value;

        read1.setReadName(QNAME + "/1");
        read1.setFlags(FLAGS1);
        read1.setReferenceName(REFERENCE);
        read1.reposition(MAPPOS1);
        read1.setMappingQuality(MAPQ1);
        read1.setCigar(CIGAR1);
        read1.setNextReferenceName(REFNEXT1);
        read1.setNextPosition(NEXTP1);
        read1.setSequence(SEQ1.getBytes(), true);
        read1.setPhred(PHRED1);

        tabCutter.set(TAGS1);
        for (int i = 0; i < tabCutter.getCutCount(); i++) {
            colonCutter.set(tabCutter.getCut(i));

            key = new Text(colonCutter.getCutRange(0, 1));
            value = new Text(colonCutter.getCut(2));

            read1.getProperties().put(key, value);
        }

        read2.setReadName(QNAME + "/2");
        read2.setFlags(FLAGS2);
        read2.setReferenceName(REFERENCE);
        read2.reposition(MAPPOS2);
        read2.setMappingQuality(MAPQ2);
        read2.setCigar(CIGAR2);
        read2.setNextReferenceName(REFNEXT2);
        read2.setNextPosition(NEXTP2);
        read2.setSequence(SEQ2.getBytes(), true);
        read2.setPhred(PHRED2);

        tabCutter.set(TAGS2);
        for (int i = 0; i < tabCutter.getCutCount(); i++) {
            colonCutter.set(tabCutter.getCut(i));

            key = new Text(colonCutter.getCutRange(0, 1));
            value = new Text(colonCutter.getCut(2));

            read2.getProperties().put(key, value);
        }

        // Both reads have the same query template.
        queryTemplate.setTemplateName(QNAME);
        queryTemplate.set(read1, read2);
    }

    public void test_Resets() throws IOException, URISyntaxException {
        _reset();

        assertEquals(QNAME + "/1", read1.getReadName());
        assertEquals(FLAGS1, read1.getFlags());
        assertEquals(REFERENCE, read1.getReferenceName());
        assertEquals(MAPPOS1, read1.getEndpoints().first());
        assertEquals(MAPQ1, read1.getMappingQuality());
        assertEquals(CIGAR1, read1.getCigar());
        assertEquals(REFNEXT1, read1.getNextReferenceName());
        assertEquals(NEXTP1, read1.getNextPosition());
        assertEquals(SEQ1, read1.getSequenceString());
        assertEquals(PHRED1, read1.getPhredString());

        // Set new values, and test again.
        read1.setReadName(QNAME + "/2");
        read1.setFlags(FLAGS2);
        read1.reposition(MAPPOS2);
        read1.setMappingQuality(MAPQ2);
        read1.setCigar(CIGAR2);
        read1.setNextPosition(NEXTP2);
        read1.setSequence(SEQ2, true);
        read1.setPhred(PHRED2);
        read1.setReferenceName("foo"); // Default read1 and read2 are the
        read1.setNextReferenceName("bar"); // same for these.

        assertEquals(QNAME + "/2", read1.getReadName());
        assertEquals(FLAGS2, read1.getFlags());
        assertEquals(MAPPOS2, read1.getEndpoints().first());
        assertEquals(MAPQ2, read1.getMappingQuality());
        assertEquals(CIGAR2, read1.getCigar());
        assertEquals(NEXTP2, read1.getNextPosition());
        assertEquals(SEQ2, read1.getSequenceString());
        assertEquals(PHRED2, read1.getPhredString());
        assertEquals("foo", read1.getReferenceName());
        assertEquals("bar", read1.getNextReferenceName());

        // Set everything back, and test one last time.
        read1.setReadName(QNAME + "/1");
        read1.setFlags(FLAGS1);
        read1.setReferenceName(REFERENCE);
        read1.reposition(MAPPOS1);
        read1.setMappingQuality(MAPQ1);
        read1.setCigar(CIGAR1);
        read1.setNextReferenceName(REFNEXT1);
        read1.setNextPosition(NEXTP1);
        read1.setSequence(SEQ1.getBytes(), true);
        read1.setPhred(PHRED1);

        assertEquals(QNAME + "/1", read1.getReadName());
        assertEquals(FLAGS1, read1.getFlags());
        assertEquals(REFERENCE, read1.getReferenceName());
        assertEquals(MAPPOS1, read1.getEndpoints().first());
        assertEquals(MAPQ1, read1.getMappingQuality());
        assertEquals(CIGAR1, read1.getCigar());
        assertEquals(REFNEXT1, read1.getNextReferenceName());
        assertEquals(NEXTP1, read1.getNextPosition());
        assertEquals(SEQ1, read1.getSequenceString());
        assertEquals(PHRED1, read1.getPhredString());
    }

    public void testQueryTemplateCalculateSize() throws IOException, URISyntaxException {
        _reset();

        PositionRange positionRange = queryTemplate.calculateTemplatePosition();

        assertEquals(TEMPLATE_LENGTH, positionRange.length());
    }

    public void testQueryTemplateClone()
            throws IOException, URISyntaxException, SecurityException, NoSuchMethodException,
            IllegalArgumentException, IllegalAccessException, InvocationTargetException {

        _reset();

        IdentityTester it;
        QueryTemplate clone = new QueryTemplate(queryTemplate);

        // First verify that the clones are, in fact, identical.
        it = new IdentityTester(clone, queryTemplate, AssertThat.ASSERT_EQUALS);
        it.setMethodAction("iterator", AssertThat.IGNORE);
        it.setMethodAction("listIterator", AssertThat.IGNORE);
        it.setMethodAction("getReadsArray", AssertThat.IGNORE);

        it.runTests();
    }

    public void testQueryTemplateReadWrite()
            throws IOException, URISyntaxException, SecurityException, NoSuchMethodException,
            IllegalArgumentException, IllegalAccessException, InvocationTargetException {

        final int iterations = 3;

        _reset();

        ByteArrayOutputStream bout;
        DataOutputStream dout = new DataOutputStream(bout = new ByteArrayOutputStream());

        for (int i = 0; i < iterations; i++) {
            queryTemplate.write(dout);
        }

        for (int i = 0; i < iterations; i++) {
            QueryTemplate clone = new QueryTemplate();
            clone.readFields(new DataInputStream(new ByteArrayInputStream(bout.toByteArray())));

            IdentityTester it = new IdentityTester(queryTemplate, clone);
            it.setMethodAction("iterator", AssertThat.IGNORE);
            it.setMethodAction("listIterator", AssertThat.IGNORE);
            it.setMethodAction("getReadsArray", AssertThat.IGNORE);

            it.runTests();
        }
    }

    public void testSequencingClone()
            throws IOException, URISyntaxException, SecurityException, NoSuchMethodException,
            IllegalArgumentException, IllegalAccessException, InvocationTargetException {

        _reset();

        IdentityTester it;
        SequencingRead clone = new SequencingRead(read1);

        // First verify that the clones are, in fact, identical.
        it = new IdentityTester(read1, clone, AssertThat.ASSERT_EQUALS);
        it.setMethodAction("getBases", AssertThat.IGNORE);
        it.setMethodAction("getRawBytes", AssertThat.IGNORE);
        it.setMethodAction("getQueryTemplate", AssertThat.ASSERT_EQUALS);

        it.runTests();

        // Modify the values in clone, and test that read1 remains unchanged.
        clone.setReadName(QNAME + "/2");
        clone.setFlags(~read1.getFlags());
        clone.reposition(MAPPOS2);
        clone.setMappingQuality(MAPQ2);
        clone.setCigar(CIGAR2);
        clone.setNextPosition(NEXTP2);
        clone.setOrientation(Orientation.MINUS);
        clone.setSequence("ACACACACACACACACACACACACACACG", true);
        clone.setPhred(PHRED2);
        clone.setReferenceName("foo"); // Default clone1 and read2 are the
        clone.setNextReferenceName("bar"); // same for these.
        clone.getProperties().put("key", "value");

        it = new IdentityTester(clone, read1, AssertThat.ASSERT_NOT_EQUAL);
        it.setMethodAction("getBases", AssertThat.IGNORE);
        it.setMethodAction("getClass", AssertThat.IGNORE);
        it.setMethodAction("getQueryTemplate", AssertThat.ASSERT_EQUALS);

        it.runTests();
    }

    public void testSequencingReadWrite()
            throws IOException, URISyntaxException, SecurityException, NoSuchMethodException,
            IllegalArgumentException, IllegalAccessException, InvocationTargetException {

        SequencingRead clone = new SequencingRead();
        ByteArrayOutputStream bout;

        _reset();

        read2 = queryTemplate.remove(1);
        read1 = queryTemplate.remove(0);

        read1.write(new DataOutputStream(bout = new ByteArrayOutputStream()));
        clone.readFields(new DataInputStream(new ByteArrayInputStream(bout.toByteArray())));

        IdentityTester it;

        it = new IdentityTester(clone, read1);
        it.setMethodAction("getBases", AssertThat.IGNORE);
        it.setMethodAction("getRawBytes", AssertThat.IGNORE);
        // Specifically not serialized
        it.setMethodAction("getQueryTemplate", AssertThat.IGNORE);
        it.runTests();

        read2.write(new DataOutputStream(bout = new ByteArrayOutputStream()));
        clone.readFields(new DataInputStream(new ByteArrayInputStream(bout.toByteArray())));

        it = new IdentityTester(clone, read2);
        it.setMethodAction("getBases", AssertThat.IGNORE);
        it.setMethodAction("getRawBytes", AssertThat.IGNORE);
        // Specifically not serialized
        it.setMethodAction("getQueryTemplate", AssertThat.IGNORE);
        it.runTests();
    }
}
