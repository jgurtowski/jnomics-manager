package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer;
import edu.cshl.schatz.jnomics.ob.SAMRecordWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 * User: Piyush Kansal
 */
public class AlignmentSortReduce extends JnomicsReducer<SamtoolsMap.SamtoolsKey,SAMRecordWritable,
        NullWritable,NullWritable> {

    private static final String _NEW_LINE_			= System.getProperty("line.separator");
    private static final String _UNMAPPED_CHR_		= "*";
    private static final String _OP_DIR_			= "mapred.output.dir";
    private static final String _HEADER_FILE_		= "header.sam";
    private static final String _UNMAPPED_			= "~~UNMAPPED";

    private FileSystem ipFs;
    private String opDir;

    @Override
    public Class getOutputKeyClass() {
        return NullWritable.class;
    }

    @Override
    public Class getOutputValueClass() {
        return NullWritable.class;
    }

    @Override
    public Class<? extends Partitioner> getPartitionerClass() {
        return SamtoolsReduce.SamtoolsPartitioner.class;
    }

    @Override
    public Class<? extends WritableComparator> getGrouperClass() {
        return SamtoolsReduce.SamtoolsGrouper.class;
    }

    @Override
    public JnomicsArgument[] getArgs() {
        return new JnomicsArgument[0];  //To change body of implemented methods use File | Settings | File Templates.
    }

    protected void setup( Context context ) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        opDir = conf.get( _OP_DIR_ );
        ipFs = FileSystem.get( conf );
    }

    protected void reduce( SamtoolsMap.SamtoolsKey key, final Iterable<SAMRecordWritable> values,
                           final Context context ) throws IOException, InterruptedException {
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
