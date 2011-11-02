
package edu.cshl.schatz.jnomics.tools.examples;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsTool;
import edu.cshl.schatz.jnomics.ob.PositionRange;
import edu.cshl.schatz.jnomics.ob.writable.QueryTemplate;
import edu.cshl.schatz.jnomics.ob.writable.SequencingRead;

/**
 * Calculates the distance between aligned read pairs (paired end sequencing)
 * One approach for the detection of structural variations is the analysis of
 * discordant pairs. Insertions and deletions cause bridging reads to align
 * closer together or farther apart than the standard insert size. This example
 * will list the distances between aligned pairs for the detection of such
 * anomalies.
 * 
 * @author James
 */
public class PairDistance extends JnomicsTool {

    public static void main(String[] args) throws Exception {
        JnomicsTool.run(new PairDistance(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = getJob();

        job.setReducerClass(PairDistanceReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(QueryTemplate.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * PairDistance Reducer
     * 
     * @author James
     */
    public static class PairDistanceReducer
            extends JnomicsReducer<Text, QueryTemplate, Text, IntWritable> {

        final IntWritable aliDist = new IntWritable();
        PositionRange range1, range2;
        SequencingRead read1, read2;

        @Override
        protected void reduce(Text key, Iterable<QueryTemplate> values, Context cxt)
                throws IOException, InterruptedException {

            for (final QueryTemplate template : values) {
                if (template.size() >= 2) {
                    read1 = template.getFirst();
                    read2 = template.getLast();
                    if (read1 != null && read2 != null) {
                        range1 = read1.getEndpoints();
                        range2 = read2.getEndpoints();
                        if (range1 != null && range2 != null) {
                            aliDist.set(Math.abs(range2.last() - range1.first()));
                            cxt.write(template.getTemplateName(), aliDist);
                        }
                    }
                }
            }
        }
    }
}