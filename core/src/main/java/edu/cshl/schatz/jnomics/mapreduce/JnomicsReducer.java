/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.mapreduce;

import static edu.cshl.schatz.jnomics.mapreduce.JnomicsJob.P_REDUCE_FAIL_IF_NO_READS_IN;
import static edu.cshl.schatz.jnomics.mapreduce.JnomicsMapper.COUNTER_GROUP;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import edu.cshl.schatz.jnomics.ob.writable.QueryTemplate;
import edu.cshl.schatz.jnomics.ob.writable.SequencingRead;

/**
 * An extension of the standard {@link Mapper} that exposes its generic type
 * parameters so that they are accessible at runtime.
 * <p>
 * TODO >90% of processor time is being spend in this method creating new
 * instances of QueryTemplate. Implement an object pool ASAP.
 * 
 * @author Matthew A. Titmus
 */
public class JnomicsReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
        extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

    private static final Log LOG = LogFactory.getLog(JnomicsReducer.class);

    private final static int VALUE_TYPE_OTHER = 3;
    private final static int VALUE_TYPE_READS = 2;
    private final static int VALUE_TYPE_TEMPLATES = 1;

    private Map<String, VALUEIN> lookup = new TreeMap<String, VALUEIN>();

    /*
     * @see org.apache.hadoop.mapreduce.Reducer#run(Context)
     */
    @SuppressWarnings({ "unchecked" })
    @Override
    public void run(Context context) throws IOException, InterruptedException {
        final Configuration config = context.getConfiguration();
        final Class<?> valueOutClass = config.getClass("mapred.output.value.class", Text.class);
        final Class<?> valueInClass = config.getClass("mapred.mapoutput.value.class", valueOutClass);

        final Counter readCounter = context.getCounter(COUNTER_GROUP, "Reducer reads in");
        final Counter templateCounter = context.getCounter(COUNTER_GROUP, "Reducer templates in");
        final Counter unknownCounter = context.getCounter(COUNTER_GROUP, "Reducer key-values in");

        int readCount = 0, templateCount = 0, unknownCount = 0, loopCounter = 0;
        int valueType = VALUE_TYPE_OTHER;

        if (QueryTemplate.class.isAssignableFrom(valueInClass)) {
            valueType = VALUE_TYPE_TEMPLATES;
        } else if (SequencingRead.class.isAssignableFrom(valueInClass)) {
            valueType = VALUE_TYPE_READS;
        }

        setup(context);

        while (context.nextKey()) {
            Iterable<VALUEIN> values = context.getValues();

            switch (valueType) {
            // If we're using query templates, merge like templates.
            case VALUE_TYPE_TEMPLATES:
                QueryTemplate template = null;

                lookup.clear();

                for (VALUEIN valuein : values) {
                    QueryTemplate value = (QueryTemplate) valuein;
                    String name = value.getTemplateNameString();

                    if (null == (template = (QueryTemplate) lookup.get(name))) {
                        lookup.put(name, (VALUEIN) (template = new QueryTemplate(value)));
                    } else {
                        Iterator<SequencingRead> iter = value.iterator();
                        while (iter.hasNext()) {
                            template.add(iter.next());
                        }

                        if ((template.size() == 2) //
                                && (template.get(0).getMappingPosition() == 0)
                                && (template.get(1).getMappingPosition() == 0)) {

                            SequencingRead a = template.get(0);
                            SequencingRead b = template.get(1);
                            a.setNextPosition(b.getMappingPosition());
                            a.setNextReferenceName(b.getReferenceNameText());
                            b.setNextPosition(a.getMappingPosition());
                            b.setNextReferenceName(a.getReferenceNameText());
                        }
                    }
                }

                values = lookup.values();
                readCount += template.size();
                templateCount++;
                break;

            case VALUE_TYPE_READS:
                templateCount++;
                break;

            default:
                unknownCount++;
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

            reduce(context.getCurrentKey(), values, context);
        } // End while (context.nextKey())

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
            String message = jobName + " (" + jobId + ") reducer received 0 inputs.";

            if (config.getBoolean(P_REDUCE_FAIL_IF_NO_READS_IN, false)) {
                throw new IOException(message);
            } else {
                LOG.warn("WARNING: " + message);
            }
        }
    }
}
