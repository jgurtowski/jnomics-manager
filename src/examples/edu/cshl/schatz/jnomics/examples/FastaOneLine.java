package edu.cshl.schatz.jnomics.examples;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import edu.cshl.schatz.jnomics.io.FastaInputFormat;
import edu.cshl.schatz.jnomics.io.FastaRecord;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsTool;

/**
 * @author james
 * Simple Fasta Reader that reads a fasta file and
 * outputs the header and sequence on the same line.
 */
public class FastaOneLine extends JnomicsTool {
	
	public static void main(String[] args) throws Exception {
		JnomicsTool.run(new FastaOneLine(), args);
	}	
	
	 @Override
	 public int run(String[] args) throws Exception {
	   Job job = getJob();
	   job.setInputFormatClass(FastaInputFormat.class);
	   job.setMapOutputKeyClass(Text.class);
	   job.setMapOutputValueClass(Text.class);
	   job.setOutputFormatClass(TextOutputFormat.class);
	   job.setMapperClass(SimpleMapper.class);
	   job.setNumReduceTasks(0);
	   
	   return job.waitForCompletion(true) ? 0 : 1;
	 }
	 
	 public static class SimpleMapper 
	 	extends Mapper<LongWritable, FastaRecord, Text, Text>{

		@Override
		protected void map(LongWritable key, FastaRecord fa, 
				   Context context)
				throws IOException, InterruptedException {
			context.write(fa.getName(),fa.getSequence());
		}
	 }
}
