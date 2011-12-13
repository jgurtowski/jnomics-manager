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

package edu.cshl.schatz.jnomics.ob.header;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMSequenceRecord;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import edu.cshl.schatz.jnomics.io.JnomicsFileRecordReader;
import edu.cshl.schatz.jnomics.io.SequencingReadInputFormat;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsJob;
import edu.cshl.schatz.jnomics.mapreduce.ReadFileFormat;

/* HOW TO USE PICARD TO GENERATE HEADERS TO AN OUTPUT STREAM
 * 
 * StringWriter headerTextBuffer = new StringWriter();
 * new SAMTextHeaderCodec().encode(headerTextBuffer, header);
 * System.out.println(headerTextBuffer.toString());
 */

/**
 * @author Matthew A. Titmus
 */
public class HeaderDataLookup {
    private static Map<String, HeaderData> headerLookup = new HashMap<String, HeaderData>(5, 0.5f);

    private static Map<String, ReferenceSequenceRecord> refSeqLookup = new HashMap<String, ReferenceSequenceRecord>(
        5, 0.5f);

    /**
     * Adds a header to the lookup table under the specified id, and
     * automatically adds all of its reference sequence data to the reference
     * sequence lookup table as well. If <code>header</code> is not an instance
     * of {@link HeaderData}, a <code>HeaderData</code> copy will be stored
     * instead.
     * 
     * @param header A
     * @throws IdConflictException
     */
    public static void addHeader(String id, SAMFileHeader header) throws IdConflictException {
        HeaderData hd = header instanceof HeaderData ? (HeaderData) header : new HeaderData(header);

        if (containsHeader(id)) {
            throw new IdConflictException("A header with id \'" + id + "\' is already present.");
        }

        headerLookup.put(id, hd);
        addReferenceSequenceRecord(hd.getSequenceDictionary().getSequences());
    }

    /**
     * Clears all header and reference sequence contents. Use with caution, obviously.
     */
    public static synchronized void clear() {
        headerLookup.clear();
        refSeqLookup.clear();
    }

    /**
     * Adds reference sequence records to the lookup map, using the reference
     * sequence names as the lookup key.
     * 
     * @param refs A {@link Collection} containing reference records to add to
     *            the lookup map.
     */
    public static void addReferenceSequenceRecord(Collection<SAMSequenceRecord> refs)
            throws IdConflictException {

        for (SAMSequenceRecord r : refs) {
            addReferenceSequenceRecord(r);
        }
    }

    /**
     * Adds a reference sequence record to the lookup map, using the reference
     * sequence name ({@line ReferenceSequenceRecord#getSequenceName()}) as the
     * lookup key.
     * 
     * @param ref The reference record to add to the lookup map.
     */
    public static void addReferenceSequenceRecord(SAMSequenceRecord ref) {
        addReferenceSequenceRecord(ref.getSequenceName(), ref);
    }

    /**
     * Adds a reference sequence record to the lookup map using the specified
     * lookup key.
     * 
     * @param id The key to associate the record with.
     * @param ref The reference record to add to the lookup map.
     */
    public static void addReferenceSequenceRecord(String id, SAMSequenceRecord ref) {
        refSeqLookup.put(id, ref instanceof ReferenceSequenceRecord
                ? (ReferenceSequenceRecord) ref
                : new ReferenceSequenceRecord(ref));
    }

    /**
     * Returns <code>true</code> if a reference sequence record is already
     * stored, using the value of
     * {@link ReferenceSequenceRecord#getSequenceName()} as the key.
     */
    public static boolean containsHeader(String id) {
        return headerLookup.containsKey(id);
    }

    /**
     * Returns <code>true</code> if a reference sequence record is already
     * stored, using the value of
     * {@link ReferenceSequenceRecord#getSequenceName()} as the key.
     */
    public static boolean containsReferenceSequence(SAMSequenceRecord ref) {
        return refSeqLookup.containsKey(ref.getSequenceName());
    }

    /**
     * Returns <code>true</code> if a reference sequence record is already
     * stored under the specified key.
     */
    public static boolean containsReferenceSequenceRecord(String id) {
        return refSeqLookup.containsKey(id);
    }

    /**
     * A convenience method for accessing file header data. If the headers for
     * <code>file</code> are not yet known, this method creates the appropriate
     * {@link JnomicsFileRecordReader} instance and calls its
     * {@link JnomicsFileRecordReader#getHeaderData(Path, Configuration)}
     * method.
     * 
     * @return A {@link HeaderData} instance, or <code>null</code> if the
     *         {@link JnomicsFileRecordReader} implementation for the file
     *         format doesn't generate headers.
     * @throws InterruptedException
     * @throws IOException
     */
    public static HeaderData getHeader(Path file, Configuration conf)
            throws IOException, InterruptedException {

        HeaderData data = null;

        synchronized (headerLookup) {
            if (headerLookup.containsKey(file.toString())) {
                data = headerLookup.get(file.toString());
            } else {
                ReadFileFormat readFormat = ReadFileFormat.get(file);
                JnomicsFileRecordReader reader = SequencingReadInputFormat.newRecordReader(readFormat);

                if (conf == null) {
                    conf = new JnomicsJob().getConfiguration();
                }
                
                reader.initialize(file, conf);

                // Null headers are cached like anything else.
                addHeader(file.toString(), data = reader.readHeaderData(file, conf));
            }
        }

        return data;
    }

    /**
     * A convenience method for accessing file header data. If the headers for
     * <code>file</code> are not yet known, this method creates the appropriate
     * {@link JnomicsFileRecordReader} instance (using a default configuration)
     * and calls its
     * {@link JnomicsFileRecordReader#getHeaderData(Path, Configuration)}
     * method.
     * 
     * @return A {@link HeaderData} instance, or <code>null</code> if the
     *         {@link JnomicsFileRecordReader} implementation for the file
     *         format doesn't generate headers.
     * @throws InterruptedException
     * @throws IOException
     */
    public static HeaderData getHeader(Path file) throws IOException, InterruptedException {
        return getHeader(file, null);
    }

    /**
     * Look up header data by its id.
     * 
     * @param id
     * @return The header data with the associated id, or <code>null</code> if
     *         no such header exists.
     */
    public static HeaderData getHeader(String id) {
        return headerLookup.get(id);
    }

    /**
     * Returns an unmodifiable {@link Map} containing all header id's and
     * associated {@link ReferenceSequenceRecord}s
     */
    public static Map<String, HeaderData> getHeaders() {
        return Collections.unmodifiableMap(headerLookup);
    }

    /**
     * Returns <code>true</code> if a reference sequence record is already
     * stored under the specified key.
     */
    public static ReferenceSequenceRecord getReferenceSequenceRecord(String id) {
        return refSeqLookup.get(id);
    }

    /**
     * Returns an unmodifiable {@link Map} containing all reference sequence
     * id's and associated {@link ReferenceSequenceRecord}s
     */
    public static Map<String, ReferenceSequenceRecord> getRefererenceSequenceRecords() {
        return Collections.unmodifiableMap(refSeqLookup);
    }
}
