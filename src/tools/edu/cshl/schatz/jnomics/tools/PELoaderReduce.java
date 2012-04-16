package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.io.FastqParser;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer;
import edu.cshl.schatz.jnomics.ob.ReadCollectionWritable;
import edu.cshl.schatz.jnomics.ob.ReadWritable;
import edu.cshl.schatz.jnomics.ob.writable.StringArrayWritable;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;

/**
 * User: james
 */
public class PELoaderReduce extends JnomicsReducer<IntWritable, StringArrayWritable, Text, NullWritable> {

    private final Text output = new Text();

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
    protected void reduce(IntWritable key, Iterable<StringArrayWritable> values, Context context) throws IOException,
            InterruptedException {

        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);

        for (StringArrayWritable maniline : values) {
            String[] data = maniline.toStrings();
            Path p1 = new Path(data[0]);
            Path p2 = new Path(data[1]);
            InputStream in1 = fs.open(p1);
            InputStream in2 = fs.open(p2);

            if (p1.toString().endsWith(".bz2")) {
                in1 = new BZip2CompressorInputStream(in1);
            } else if (p1.toString().endsWith(".gz")) {
                in1 = new GZIPInputStream(in1);
            }
            if (p2.toString().endsWith(".bz2")) {
                in2 = new BZip2CompressorInputStream(in2);
            } else if (p2.toString().endsWith(".gz")) {
                in2 = new GZIPInputStream(in2);
            }

            /**Could construct new output format but we'll do it
             * the dirty way for now
             */
            String outName = p1.getName() + ".pe";
            Path outPath = new Path(conf.get("mapred.output.dir","") +"/"+ outName);
            
            FastqParser parser1 = new FastqParser(in1);
            FastqParser parser2 = new FastqParser(in2);
            SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, outPath,
                    ReadCollectionWritable.class,
                    NullWritable.class);


            ReadCollectionWritable readCollection = new ReadCollectionWritable();

            ReadWritable r1= new ReadWritable();
            ReadWritable r2 = new ReadWritable();
            readCollection.addRead(r1);
            readCollection.addRead(r2);
            Text keyName = new Text();
            readCollection.setName(keyName);

            Iterator<FastqParser.FastqRecord> it1 = parser1.iterator();
            Iterator<FastqParser.FastqRecord> it2 = parser2.iterator();

            FastqParser.FastqRecord record1,record2;
            System.out.println(outName);

            int counter = 1;
            output.set("Loaded " + outPath.toString());
            context.write(output,NullWritable.get());
            while(it1.hasNext() && it2.hasNext()){
                record1 = it1.next();
                record2 = it2.next();
                keyName.set(Integer.toString(counter));
                r1.setAll(record1.getName(), record1.getSequence(), record1.getDescription(), record1.getQuality());
                r2.setAll(record2.getName(),record2.getSequence(),record2.getDescription(),record2.getQuality());
                writer.append(readCollection,NullWritable.get());
                counter++;
                if(counter % 100000 == 0){
                    System.out.println(counter);
                    output.set(Integer.toString(counter));
                    context.write(output,NullWritable.get());
                    context.progress();
                }
            }
            parser1.close();
            parser2.close();
            writer.close();
        }
    }
}
