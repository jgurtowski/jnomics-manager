package edu.cshl.schatz.jnomics.test.util;

import edu.cshl.schatz.jnomics.util.SequenceOpts;
import junit.framework.TestCase;

/**
 * User: james
 */
public class SequenceOptsTest extends TestCase{

    public void testReverseComplement(){
        assertEquals("ACGTACGT",SequenceOpts.reverseComplement("ACGTACGT"));
    }

}
