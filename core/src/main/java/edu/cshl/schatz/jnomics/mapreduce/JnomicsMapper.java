/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.mapreduce;

import static edu.cshl.schatz.jnomics.mapreduce.JnomicsJob.P_MAP_FAIL_IF_NO_READS_IN;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import edu.cshl.schatz.jnomics.ob.writable.QueryTemplate;
import edu.cshl.schatz.jnomics.ob.writable.SequencingRead;

/**
 * An extension of the standard {@link Mapper} that exposes its generic type
 * parameters so that they are accessible at runtime.
 * 
 * @author Matthew A. Titmus
 */
public class JnomicsMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
        extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

    private static final Log LOG = LogFactory.getLog(JnomicsMapper.class);

    static final String COUNTER_GROUP = "Jnomics Framework";

    private final static int VALUE_TYPE_OTHER = 3;
    private final static int VALUE_TYPE_READS = 2;
    private final static int VALUE_TYPE_TEMPLATES = 1;

    /*
     * @see
     * org.apache.hadoop.mapreduce.Mapper#run(org.apache.hadoop.mapreduce.Mapper
     * .Context)
     */
    @SuppressWarnings("unchecked")
    @Override
    public void run(Context context) throws IOException, InterruptedException {        
        final Configuration config = context.getConfiguration();
        final Class<?> valueOutClass = config.getClass("mapred.output.value.class", Text.class);
        final Class<?> valueInClass = config.getClass("mapred.mapoutput.value.class", valueOutClass);

        final Counter readCounter = context.getCounter(COUNTER_GROUP, "Mapper reads in");
        final Counter templateCounter = context.getCounter(COUNTER_GROUP, "Mapper templates in");
        final Counter unknownCounter = context.getCounter(COUNTER_GROUP, "Mapper key-values in");

        int readCount = 0, templateCount = 0, unknownCount = 0, loopCounter = 0;
        int size, valueType = VALUE_TYPE_OTHER;

        if (QueryTemplate.class.isAssignableFrom(valueInClass)) {
            valueType = VALUE_TYPE_TEMPLATES;
        } else if (SequencingRead.class.isAssignableFrom(valueInClass)) {
            valueType = VALUE_TYPE_READS;
        }

        setup(context);

        while (context.nextKeyValue()) {
            QueryTemplate value = (QueryTemplate) context.getCurrentValue();
            readCount += (size = value.size());
            
            switch (valueType) {
            case VALUE_TYPE_TEMPLATES:
                templateCount += 1;
                map(context.getCurrentKey(), (VALUEIN) value, context);
                break;

            case VALUE_TYPE_READS:
                SequencingRead[] reads = value.getReadsArray();

                for (int i = 0; i < size; i++) {
                    map(context.getCurrentKey(), (VALUEIN) reads[i], context);
                }

                break;

            default:
                unknownCount++;
                map(context.getCurrentKey(), (VALUEIN) value, context);
            }

            // Update the counters after every 1000 iterations
            if (0 == (++loopCounter % 1000)) {
                if (valueType != VALUE_TYPE_OTHER) {
                    templateCounter.increment(templateCount);
                    readCounter.increment(readCount);
                } else {
                    unknownCounter.increment(unknownCount);
                }

                unknownCount = readCount = templateCount = 0;
            }
        } // End while (context.nextKeyValue())

        if (valueType != VALUE_TYPE_OTHER) {
            templateCounter.increment(templateCount);
            readCounter.increment(readCount);
        } else {
            unknownCounter.increment(unknownCount);
        }

        cleanup(context);

        if (loopCounter == 0) {
            String jobName = config.get("mapred.job.name", "Unnamed Jnomics job");
            String jobId = context.getJobID().toString();
            String message = jobName + " (" + jobId + ") mapper received 0 input reads.";

            if (config.getBoolean(P_MAP_FAIL_IF_NO_READS_IN, true)) {
                throw new IOException(message);
            } else {
                LOG.warn("WARNING: " + message);
            }
        }
    }
}