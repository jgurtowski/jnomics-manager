package edu.cshl.schatz.jnomics.io;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * @author james
 * A FileInputFormat for reading Fasta Files
 * Gives the mapper a record view of the Fasta data
 * General Hadoop Note: You will get one mapper per fasta
 * entry. Large entries (ie. hg19) are inefficient because only
 * one Mapper is run per chromosome. Consider splitting large
 * records like this into smaller entries that can be processed
 * independently.
 * @return FastaRecordReader
 */
public class FastaInputFormat extends FileInputFormat<LongWritable, FastaRecord> {

	@Override
	public RecordReader<LongWritable, FastaRecord> createRecordReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException,
			InterruptedException {
		return new FastaRecordReader();
	}
	
}
