/*
 * This file is part of Jnomics.
 * Copyright 2011 Matthew A. Titmus
 * All rights reserved.
 *  
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *       
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *       
 *     * Neither the name of the Cold Spring Harbor Laboratory nor the names of
 *       its contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package edu.cshl.schatz.jnomics.test.core;

import junit.framework.TestCase;
import net.sf.samtools.SAMSequenceRecord;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import edu.cshl.schatz.jnomics.ob.header.HeaderData;
import edu.cshl.schatz.jnomics.ob.header.HeaderDataLookup;
import edu.cshl.schatz.jnomics.ob.header.ReferenceSequenceRecord;

/**
 * @author Matthew A. Titmus
 */
public class HeaderLookupTest extends TestCase {
    static final String EXAMPLE_DATA = "./example-data/inputFormats/";

    @Test
    public void testBAMHeaders() throws Exception {
        Path file = new Path(EXAMPLE_DATA + "example.bam");

        doTestFileHeaders(file);
        doTestReferenceSequences(file);
    }

    @Test
    public void testFastqHeaders() throws Exception {
        doTestFileHeaders( //
            new Path(EXAMPLE_DATA + "example.1.fq"), //
            new Path(EXAMPLE_DATA + "example.1.fq"));
    }

    @Test
    public void testFastaHeaders() throws Exception {
        Path file = new Path(EXAMPLE_DATA + "example.fa");

        doTestFileHeaders(file);
    }

    @Test
    public void testBEDHeaders() throws Exception {
        Path file = new Path(EXAMPLE_DATA + "example.bed");

        doTestFileHeaders(file);
    }

    @Test
    public void testSAMHeaders() throws Exception {
        Path file = new Path(EXAMPLE_DATA + "example.sam");

        doTestFileHeaders(file);
        doTestReferenceSequences(file);
    }

    private void doTestFileHeaders(Path... files) throws Exception {
        HeaderDataLookup.clear();

        for (Path file : files) {
            HeaderData data1 = HeaderDataLookup.getHeader(file);
            HeaderData data2 = HeaderDataLookup.getHeader(file.toString());

            // If the headers are cached correctly, then they should be the
            // same instance.
            assertSame(data1, data2);

        }
    }

    private void doTestReferenceSequences(Path file) throws Exception {
        HeaderDataLookup.clear();

        HeaderData data = HeaderDataLookup.getHeader(file);

        // Expect reference sequences
        assertTrue(
            "HeaderData " + file.toString() + " contains no reference sequences",
            data.getSequenceDictionary().getSequences().size() > 0);

        // Each reference sequence in the header should be accessible via
        // the getReferenceSequenceRecord method, and the header's instance
        // should be identical to the cached one.

        for (SAMSequenceRecord ref : data.getSequenceDictionary().getSequences()) {
            ReferenceSequenceRecord cached;

            assertTrue(HeaderDataLookup.containsReferenceSequence(ref));
            assertNotNull(cached = HeaderDataLookup.getReferenceSequenceRecord(ref.getId()));
            assertSame(
                "getReferenceSequenceRecord(String) returned the wrong record instance", ref,
                cached);
        }
    }
}
