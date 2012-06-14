package edu.cshl.schatz.jnomics.ob;

import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;

import java.io.ByteArrayInputStream;

/**
 * User: james
 */

public class SAMRecordWritableTest extends junit.framework.TestCase {

    private String testHeader;
    private String testString;
    
    @Override
    protected void setUp() throws Exception {
        testHeader = "@HD\tVN:1.0\tSO:unsorted\n";
        testHeader += "@SQ\tSN:chr10\tLN:2900\n";
        testHeader += "@SQ\tSN:chr1\tLN:2900\n";
        testHeader += "@SQ\tSN:chr7\tLN:2900\n";
        testHeader += "@PG\tID:bowtie2\tPN:bowtie2\tVN:2.0.0-beta4";

        testString = testHeader + "\n";
        testString += "chr10_2274_2763_1:0:0_0:0:0_0\t83\tchr10\t2694\t42\t70M\t=\t2274\t-490\tAATCCCGGCACTTTGGGAGGCCGAGGCGGGTGGATCACCTGAGGTCACGACTTCAAGACTAGCCTGACCA\t2222222222222222222222222222222222222222222222222222222222222222222222\tAS:i:0\tXN:i:0\tXM:i:0\tXO:i:0\tXG:i:0\tNM:i:0\tMD:Z:70\tYS:i:-3\tYT:Z:CP\n";
        testString += "chr10_2274_2763_1:0:0_0:0:0_0\t163\tchr10\t2274\t42\t70M\t=\t2694\t490\tTGTACATAAAATAAAAAATAAAAAAAAAGGAGTTCCTGGATTCTACACATTTTCTTTTTTAACAGCATTG\t2222222222222222222222222222222222222222222222222222222222222222222222\tAS:i:-3\tXN:i:0\tXM:i:1\tXO:i:0\tXG:i:0\tNM:i:1\tMD:Z:6A63\tYS:i:0\tYT:Z:CP\n";
        testString += "chr10_1093_1570_1:0:0_1:0:0_1\t83\tchr10\t1501\t42\t70M\t=\t1093\t-478\tATTTTATGTAGGATTTTTATATCACTATTTCTACAGGTCACTGATTTTCTTTGTATATGCTGTTTTGTGG\t2222222222222222222222222222222222222222222222222222222222222222222222\tAS:i:-3\tXN:i:0\tXM:i:1\tXO:i:0\tXG:i:0\tNM:i:1\tMD:Z:54C15\tYS:i:-3\tYT:Z:CP\n";
        testString += "chr10_282_725_1:0:0_1:0:0_9\t99\tchr10\t282\t42\t70M\t=\t656\t444\tTTTAAAAGTATAAATCGTCTAGCTAATAGATACAAATGAAATTCCAAGGGGCATACTGTGGAAAAAAGTC\t2222222222222222222222222222222222222222222222222222222222222222222222\tAS:i:-3\tXN:i:0\tXM:i:1\tXO:i:0\tXG:i:0\tNM:i:1\tMD:Z:10A59\tYS:i:-3\tYT:Z:CP\n";
    }

    public void testSet() throws Exception {
        SAMFileReader samReader = new SAMFileReader(new ByteArrayInputStream(testString.getBytes()));
        SAMRecordWritable samWritable;
        for(SAMRecord samRecord : samReader){
            samWritable = new SAMRecordWritable();
            samWritable.set(samRecord);
            assertEquals(samRecord.getSAMString().trim(),samWritable.toString());
            assertEquals(testHeader, samWritable.getTextHeader().toString());
        }
    }
    
    public void testSequenceHeaderGetter(){
        SAMFileReader samReader = new SAMFileReader(new ByteArrayInputStream(testString.getBytes()));
        SAMRecordWritable samWritable = new SAMRecordWritable();
        for(SAMRecord samRecord: samReader){
            samWritable.set(samRecord);
        }
        System.out.println(samWritable.getHeaderSequence("chr1").getLength());
    }

    public void testTags(){
        SAMFileReader samReader = new SAMFileReader(new ByteArrayInputStream(testString.getBytes()));
        SAMRecordWritable samWritable;
        SAMRecordWritable writable = new SAMRecordWritable();
        for(SAMRecord samRecord : samReader){
            writable.set(samRecord);
            System.out.println(writable.getTags().toString());
        }
    }
}
