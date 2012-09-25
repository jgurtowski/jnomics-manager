package edu.cshl.schatz.jnomics.test.core;

import edu.cshl.schatz.jnomics.util.Nucleotide;
import junit.framework.TestCase;

/**
 * User: james
 */
public class NucelotideTest extends TestCase {

    private static final String sequence = "ACGAAANGANGCCNAGAGNNNACAACACA";
    private static final String sequence_rev = "TGTGTTGTNNNCTCTNGGCNTCNTTTCGT";
    
    public void testReverseComplement(){
        assertEquals(sequence_rev, Nucleotide.reverseComplement(sequence));
    }

}
