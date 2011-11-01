/*
 * This file is part of Jnomics.test.
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

import static edu.cshl.schatz.jnomics.test.core.AbstractReadInputFormatTest.FULL_FILE_PATH;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import edu.cshl.schatz.jnomics.io.JnomicsFileRecordReader;
import edu.cshl.schatz.jnomics.io.SAMRecordWriter;
import edu.cshl.schatz.jnomics.io.SequencingReadInputFormat;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsRecordWriter;
import edu.cshl.schatz.jnomics.mapreduce.ReadFileFormat;
import edu.cshl.schatz.jnomics.ob.writable.QueryTemplate;
import edu.cshl.schatz.jnomics.util.TextCutter;

/**
 * @author Matthew A. Titmus
 */
public class SAMOutputFormatTest extends TestCase {
    static final String GENERATED_DIR = "/tmp/inputFormats/";
    static final String GENERATED_FILE = "generated.sam";
    static final String STANDARD_SAM = FULL_FILE_PATH + "example.sam";

    public void testWriteFormat() throws IOException, InterruptedException {
        File generatedFile = new File(GENERATED_DIR, GENERATED_FILE);
        File expectedFile = new File(STANDARD_SAM);

        Configuration conf = new Configuration();
        QueryTemplate standard = new QueryTemplate();
        JnomicsFileRecordReader samStandard;
        JnomicsRecordWriter<Text, QueryTemplate> writer;

        generatedFile.getParentFile().mkdirs();

        samStandard = SequencingReadInputFormat.newRecordReader(ReadFileFormat.SAM);
        samStandard.initialize(new Path(STANDARD_SAM), conf);

        writer = new SAMRecordWriter<Text, QueryTemplate>(new DataOutputStream(
            new FileOutputStream(generatedFile)), conf);

        while (samStandard.nextKeyValue()) {
            standard.set(samStandard.getCurrentValue());
            writer.write(standard.getTemplateName(), standard);
        }
        BufferedReader ain = null, ein = null;

        ain = new BufferedReader(new FileReader(generatedFile));
        ein = new BufferedReader(new FileReader(expectedFile));

        String actual = ain.readLine();
        String expected = ein.readLine();
        int lineCounter = 1;
        TextCutter cutter = new TextCutter().setDelimiter('\t');
        
        // TODO Remove when we get the header writes working
        while (expected.startsWith("@")) {
            expected = ein.readLine();
        }

        while ((null != actual) || (null != expected)) {
            actual = cutter.set(actual).getCutRange(0, 10).toString();
            expected = cutter.set(expected).getCutRange(0, 10).toString();
            
            org.junit.Assert.assertEquals("[Line " + lineCounter + "]", expected, actual);

            actual = ain.readLine();
            expected = ein.readLine();

            lineCounter++;
        }
    }
}
