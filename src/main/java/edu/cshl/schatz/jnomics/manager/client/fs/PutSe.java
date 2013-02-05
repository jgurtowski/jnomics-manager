package edu.cshl.schatz.jnomics.manager.client.fs;

import edu.cshl.schatz.jnomics.io.FastqParser;
import edu.cshl.schatz.jnomics.manager.client.ann.Flag;
import edu.cshl.schatz.jnomics.manager.fs.JnomicsThriftFileSystem;
import edu.cshl.schatz.jnomics.ob.ReadCollectionWritable;
import edu.cshl.schatz.jnomics.ob.ReadWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

/**
 * User: james
 */
public class PutSe extends FSBase{

    @Flag(shortForm = "-h", longForm = "--help")
    public boolean help;

    @Override
    public void handle(List<String> remainingArgs, Properties properties) throws Exception {
        super.handle(remainingArgs, properties);

        if(remainingArgs.size() < 2 || help){
            System.out.println("fs -put_se <reads.fq> <output.se>");
        }else{

            InputStream infile1 = edu.cshl.schatz.jnomics.util.FileUtil.getInputStreamWrapperFromExtension(
                    new FileInputStream(remainingArgs.get(0)),
                    edu.cshl.schatz.jnomics.util.FileUtil.getExtension(remainingArgs.get(0)));
            FastqParser parser1 = new FastqParser(infile1);
            String outFile = remainingArgs.get(1).concat(".se");

            NullWritable sfValue = NullWritable.get();
            ReadCollectionWritable sfKey  = new ReadCollectionWritable();

            ReadWritable r1= new ReadWritable();
            sfKey.addRead(r1);
            Text keyName = new Text();
            sfKey.setName(keyName);

            JnomicsThriftFileSystem fs = new JnomicsThriftFileSystem(client,auth);

            SequenceFile.Writer writer = SequenceFile.createWriter(fs,new Configuration(),new Path(outFile),
                    sfKey.getClass(),sfValue.getClass());


            long count = 0;
            for(FastqParser.FastqRecord record: parser1){
                r1.setAll(record.getName(),record.getSequence(),record.getDescription(),record.getQuality());
                keyName.set(record.getName());
                writer.append(sfKey,sfValue);
                if(0 == ++count % 100000){
                    System.out.println(count);
                }
            }

            parser1.close();
            infile1.close();
            writer.close();

        }
    }
}
