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

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.fs.Path;

import edu.cshl.schatz.jnomics.mapreduce.JnomicsMapper;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsTool;
import edu.cshl.schatz.jnomics.ob.writable.QueryTemplate;
import edu.cshl.schatz.jnomics.ob.writable.SequencingRead;
import edu.cshl.schatz.jnomics.tools.PropertyQuery;

/**
 * @author Matthew A. Titmus
 */
public class ReadCastTest extends TestCase {
    static final String IN = "./test-data/inputFormats/example.1.fq";

    static final String OUT = "/tmp/test";

    private static final String ARGS[] = String.format("-in %s -out %s", IN, OUT).split(" ");

    /**
     * 
     */
    public ReadCastTest() {}

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        ReadCastTest test = new ReadCastTest();

        test.testSR();
        test.testQT();
    }

    public void testQT() throws Exception {
        runTest(new QT_Tool());
    }

    public void testSR() throws Exception {
        runTest(new SR_Tool());
    }

    private void runTest(JnomicsTool tool) throws Exception {
        long millis;

        Path p = new Path(OUT);
        p.getFileSystem(tool.getConf()).delete(p, true);

        millis = System.currentTimeMillis();
        JnomicsTool.run(tool, ARGS);

        System.out.println("Total time (ms) = " + (System.currentTimeMillis() - millis));
    }

    public static class QT_Mapper
            extends JnomicsMapper<Object, QueryTemplate, Object, QueryTemplate> {

        int count = 0;

        /*
         * @see
         * org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce
         * .Mapper.Context)
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            System.out.println("MAPS=" + count);
        }

        /*
         * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
         * java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(Object key, QueryTemplate value, Context context)
                throws IOException, InterruptedException {

            count++;
            context.write(key, value);
        }
    }

    public static class QT_Reducer
            extends JnomicsReducer<Object, QueryTemplate, Object, QueryTemplate> {

        int count = 0;

        /*
         * @see
         * org.apache.hadoop.mapreduce.Reducer#cleanup(org.apache.hadoop.mapreduce
         * .Reducer.Context)
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            System.out.println("VALUES=" + count);
        }

        /*
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
         * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        @Override
        protected void reduce(Object key, Iterable<QueryTemplate> values, Context context)
                throws IOException, InterruptedException {

            for (QueryTemplate value : values) {
                count++;
                context.write(key, value);
            }
        }
    }

    public static class QT_Tool extends JnomicsTool {
        /*
         * @see
         * edu.cshl.schatz.jnomics.mapreduce.JnomicsTool#run(java.lang.String[])
         */
        @Override
        public int run(String[] args) throws Exception {
            getJob().setOutputValueClass(QueryTemplate.class);
            getJob().setMapperClass(QT_Mapper.class);
            getJob().setReducerClass(QT_Reducer.class);

            assertEquals(
                QueryTemplate.class.getName(),
                PropertyQuery.query(getConf(), "mapred.output.value.class"));

            return getJob().waitForCompletion(false) ? 0 : 1;
        }
    }

    public static class SR_Mapper
            extends JnomicsMapper<Object, SequencingRead, Object, SequencingRead> {

        int count = 0;

        /*
         * @see
         * org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce
         * .Mapper.Context)
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            System.out.println("MAPS=" + count);
        }

        /*
         * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
         * java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(Object key, SequencingRead value, Context context)
                throws IOException, InterruptedException {

            count++;
            context.write(key, value);
        }
    }

    public static class SR_Reducer
            extends JnomicsReducer<Object, SequencingRead, Object, SequencingRead> {

        int count = 0;

        /*
         * @see
         * org.apache.hadoop.mapreduce.Reducer#cleanup(org.apache.hadoop.mapreduce
         * .Reducer.Context)
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            System.out.println("VALUES=" + count);
        }

        /*
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
         * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        @Override
        protected void reduce(Object key, Iterable<SequencingRead> values, Context context)
                throws IOException, InterruptedException {

            for (SequencingRead value : values) {
                count++;
                context.write(key, value);
            }
        }
    }

    public static class SR_Tool extends JnomicsTool {
        /*
         * @see
         * edu.cshl.schatz.jnomics.mapreduce.JnomicsTool#run(java.lang.String[])
         */
        @Override
        public int run(String[] args) throws Exception {
            getJob().setOutputValueClass(SequencingRead.class);
            getJob().setMapperClass(SR_Mapper.class);
            getJob().setReducerClass(SR_Reducer.class);

            assertEquals(
                SequencingRead.class.getName(),
                PropertyQuery.query(getConf(), "mapred.output.value.class"));

            return getJob().waitForCompletion(false) ? 0 : 1;
        }
    }
}
