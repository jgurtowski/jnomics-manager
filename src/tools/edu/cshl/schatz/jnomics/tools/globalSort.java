/**
 * 
 * @file	-	globalSort.java
 * 
 * @purpose	-	Global sorting of files in following format:
 * 				1. SAMRecordWritable - uses RNAME and POS as key
 * 
 * @author 	-	Piyush Kansal
 * 
 */

package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.OptionBuilder;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsTool;
import edu.cshl.schatz.jnomics.ob.SAMRecordWritable;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.*;

/**
 * 
 * @class	-	globalSort
 * @purpose	-	To define a class to implement global sorting
 * @author 	-	Piyush Kansal
 *
 */
public class globalSort extends JnomicsTool{

	private static final String 			GS_BINSIZE 			= "binSize";
	private static final String 			GS_BINSIZE_CONF 	= "globalSort.binSize";

	private static final String 			GS_REDCNT 			= "reducerCount";
	private static final String 			GS_REDCNT_CONF 		= "globalSort.reducerCount";
	private static final String 			GS_REDCNT_DFL 		= "100";
	
	private static final String 			_NEW_LINE_			= "\n";
	private static final String 			_UNMAPPED_CHR_		= "*";
	private static final String 			_OP_DIR_			= "mapred.output.dir";
	private static final String 			_COMPRESS_MAP_OP_	= "mapreduce.map.output.compress";

	/*
	 * Make sure that this name remains same in globalSortExt.java
	 */
	private static final String 			_HEADER_FILE_		= "header.sam";
	private static final String 			_UNMAPPED_			= "~~UNMAPPED";
	
	public static void main(String[] args) throws Exception {

		int exitCode = JnomicsTool.run( new globalSort(), args );
		System.exit( exitCode );
	}

	/**
	 * Define the job configuration
	 */
	public int run( String[] args ) throws Exception {
		Configuration configuration = getConf();

		if( null == configuration.get( GS_REDCNT_CONF ) ) {
			configuration.set( GS_REDCNT_CONF, GS_REDCNT_DFL );
		}

		configuration.set( _COMPRESS_MAP_OP_, "true" );

		Job job = new Job( configuration );

		DistributedCache.createSymlink(configuration);

		job.setMapperClass(globalSort.gsMap.class);
		job.setPartitionerClass(globalSort.gsPartitioner.class);
		job.setGroupingComparatorClass(globalSort.gsGrouper.class);
		job.setReducerClass(globalSort.gsReduce.class);
	
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapOutputKeyClass(SamtoolsKey.class);
		job.setMapOutputValueClass(SAMRecordWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass( NullWritable.class );
        job.setOutputFormatClass( NullOutputFormat.class);
        //TextOutputFormat.setCompressOutput(job, true);
        //TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        
        job.setNumReduceTasks( Integer.parseInt( configuration.get( GS_REDCNT_CONF ) ) );
        job.setJarByClass( globalSort.class );

		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * Define the input arguments
	 */
	@Override
	public Options getOptions() {
       Options options = new Options();
        OptionBuilder optionBuilder = new OptionBuilder();
        
        options.addOption(optionBuilder
                .withArgName("bin-size")
                .withDescription("Bin size (Required)")
                .hasArg()
                .isRequired(true)
                .create( GS_BINSIZE ));
                
        options.addOption(optionBuilder
                .withArgName("reducer-count")
                .withDescription("Reducer count (Optional. Default value is 100)")
                .hasArg()
                .isRequired(false)
                .create( GS_REDCNT ));

        return options;
    }

	@Override
	public int handleOptions(CommandLine cmd) throws ParseException, IOException {
		for (String unrecognizedOption : cmd.getArgs()) {
            if (unrecognizedOption.startsWith("-")) {
                throw new ParseException("Unrecognized option " + unrecognizedOption);
            }
        }
        
        for (Option o : cmd.getOptions()) {
            String opt = o.getOpt();
            
            if (opt.equals( GS_BINSIZE )) {
                getConf().set( GS_BINSIZE_CONF, o.getValue() );
            }
            else if (opt.equals( GS_REDCNT )) {
                getConf().set( GS_REDCNT_CONF, o.getValue() );
            }
            else{
                System.out.println( "Unhandled parameter: " + opt + " " + o.getValue() );
            }
        }

        return STATUS_OK;
	}

	/**
	 * Define the SamtoolsKey
	 */
    public static class SamtoolsKey implements WritableComparable<SamtoolsKey> {
    	
        private Text ref = new Text();
        private IntWritable bin = new IntWritable();
        private IntWritable position = new IntWritable();

        public SamtoolsKey(){}

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            ref.write(dataOutput);
            bin.write(dataOutput);
            position.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            ref.readFields(dataInput);
            bin.readFields(dataInput);
            position.readFields(dataInput);
        }

        @Override
        public int compareTo(SamtoolsKey o) {
            int diff;
            if((diff = ref.compareTo(o.getRef())) != 0)return diff;
            if((diff = bin.compareTo(o.getBin())) != 0)return diff;
            return position.compareTo(o.getPosition());
        }

        public String toString(){
        	return ref.toString() + "-" + bin.toString() + "-" + position.toString();
        }
        
        public void setRef(String ref){
            this.ref.set(ref);
        }

        public void setBin(int bin){
            this.bin.set(bin);
        }

        public void setPosition(int position){
            this.position.set(position);
        }

        public void setRef(Text ref){
            this.ref.set(ref);
        }

        public Text getRef() {
            return ref;
        }

        public IntWritable getBin() {
        	return bin;
        }

        public IntWritable getPosition() {
            return position;
        }
    }

	/**
	 * Define the Mapper class
	 */
	public static class gsMap extends Mapper<SAMRecordWritable, NullWritable, SamtoolsKey, SAMRecordWritable> {
	
		private final SamtoolsKey stkey = new SamtoolsKey();
		private int binsize;

		protected void setup( final Context context ) throws IOException, InterruptedException {	
			binsize = Integer.parseInt( context.getConfiguration().get( GS_BINSIZE_CONF ) );
		}
		
		public void map( SAMRecordWritable key, NullWritable value, Context context ) throws IOException, InterruptedException {

			int alignmentStart = key.getAlignmentStart().get();
			stkey.setPosition( alignmentStart );
			stkey.setRef( key.getReferenceName() );

			int bin = alignmentStart / binsize;
			stkey.setBin( bin );

			context.write( stkey, key );
        }
	}

	/**
	 * Define the gsPartitioner class
	 * Currently the partitioning and sorting is based on 3 fields:
	 * 	- bin
	 * 	- RNAME
	 * 	- POS
	 */
	private static class gsPartitioner extends Partitioner<SamtoolsKey,SAMRecordWritable> {
	    
		private HashPartitioner<String, NullWritable> partitioner = new HashPartitioner<String, NullWritable>();
	
		@Override
		public int getPartition( SamtoolsKey samtoolsKey, SAMRecordWritable samRecordWritable, int i ) {
			String ref = samtoolsKey.getRef().toString();
			String bin = String.valueOf( samtoolsKey.getBin().get() );
            return partitioner.getPartition( ref + "-" + bin, NullWritable.get(), i );
		}
	}

    private static class gsGrouper extends WritableComparator{

        public gsGrouper(){
            super( SamtoolsKey.class, true );
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b){
            SamtoolsKey first = (SamtoolsKey)a;
            SamtoolsKey second = (SamtoolsKey)b;
            int diff;
            if((diff=first.getRef().compareTo(second.getRef())) == 0)
                diff =first.getBin().compareTo(second.getBin());
            return diff;
        }
    }

	/**
	 * Define the Reducer class
	 */
	public static class gsReduce extends Reducer<SamtoolsKey, SAMRecordWritable, NullWritable, NullWritable> {
		
		private FileSystem 		ipFs;
		private String			opDir;
		
		protected void setup( Context context ) throws IOException, InterruptedException {
			
			Configuration conf = context.getConfiguration();
			opDir = conf.get( _OP_DIR_ );
			ipFs = FileSystem.get( conf );
		}
		
		protected void reduce( SamtoolsKey key, final Iterable<SAMRecordWritable> values, final Context context ) throws IOException, InterruptedException {

			/*
			 * Dump the SAM header in a separate file. This header will 
			 * also have the sorting order of all the chromosomes 
			 */
			boolean headerWritten = false;
			Path samHeader = new Path( opDir + "/" + _HEADER_FILE_ );
			BufferedWriter samHeaderOut = null;
			if( !ipFs.exists( samHeader ) ) {
				samHeaderOut = new BufferedWriter( new OutputStreamWriter( ipFs.create( samHeader ) ) );
			}

			String fileName = "";
			String alnStart = key.getPosition().toString();
			String chr = key.getRef().toString();
			
			if( chr.equals( _UNMAPPED_CHR_ ) ) {
				fileName = _UNMAPPED_;
			}
			else {
				fileName = chr + "-" + alnStart;
			}
			
//			OutputStream out = ipFs.create( new Path( opDir + "/" + fileName ) );
//			if( null == out ) {
//				System.out.println( "out null" );
//			}
//
//			CompressionCodec codec = new GzipCodec(); 
//			// Or other codec. See also, CompressionCodecFactory class for some helpers.
//			if( null == codec ) {
//				System.out.println( "codec null" );
//			}
//			
//			OutputStream cs = codec.createOutputStream( out );
//			if( null == cs ) {
//				System.out.println( "cs null" );
//			}

			BufferedWriter cout = new BufferedWriter( new OutputStreamWriter( ipFs.create( new Path( opDir + "/" + fileName ) ) ) );
			if( null == cout ) {
				System.out.println( "cout null" );
			}

			while( values.iterator().hasNext() ) {
				SAMRecordWritable temp = values.iterator().next();
				cout.write( temp.toString() + _NEW_LINE_ );
				
				if( !headerWritten && ( null != samHeaderOut ) ) {
					samHeaderOut.write( temp.getTextHeader().toString() + _NEW_LINE_ );
					samHeaderOut.close();
					headerWritten = true;
				}
			}
			
			cout.close();
		}
	}
}
