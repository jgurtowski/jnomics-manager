package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer;
import edu.cshl.schatz.jnomics.ob.writable.PEMetaInfo;
import edu.cshl.schatz.jnomics.util.FileUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.InputStream;

/**
 * User: james
 */
public class PELoaderReduce extends JnomicsReducer<IntWritable, PEMetaInfo, Text, NullWritable> {

    final Log LOG = LogFactory.getLog(PELoaderReduce.class);

    @Override
    public Class getOutputKeyClass() {
        return Text.class;
    }

    @Override
    public Class getOutputValueClass() {
        return NullWritable.class;
    }

    @Override
    public JnomicsArgument[] getArgs() {
        return new JnomicsArgument[0];
    }

    @Override
    protected void reduce(IntWritable key, Iterable<PEMetaInfo> values, final Context context) throws IOException,
            InterruptedException {

        Configuration conf = context.getConfiguration();

        for (PEMetaInfo peInfo : values) {
            Path s3p1 = new Path(peInfo.getFirstFile());
            Path s3p2 = new Path(peInfo.getSecondFile());
            context.write(new Text(s3p1.toString()),NullWritable.get());
            context.write(new Text(s3p2.toString()),NullWritable.get());

            FileSystem fileSystem = FileSystem.get(s3p1.toUri(),conf);
            InputStream in1 = FileUtil.getInputStreamWrapperFromExtension(fileSystem.open(s3p1),
                    FileUtil.getExtension(s3p1.getName()));
            InputStream in2 = FileUtil.getInputStreamWrapperFromExtension(fileSystem.open(s3p2),
                    FileUtil.getExtension(s3p2.getName()));

            Path outPath = new Path(peInfo.getDestination());
            if(fileSystem.exists(outPath)) //if process fails and is rescheduled, remove old broken files
                fileSystem.delete(outPath,false);
            
            try{
                new PairedEndLoader(){
                    @Override
                    protected void progress() {
                        context.progress();
                        try {
                            context.write(new Text("working"),NullWritable.get());
                        }catch(Exception e){
                        }
                    }
                    @Override
                    protected Configuration getConf(){
                        return context.getConfiguration();
                    }
                }.load(in1,in2,outPath);
            }catch(Exception e){
                LOG.error(e.toString());
                System.err.println(e.toString());
                continue; // go to next file
            }
        }
    }
}
