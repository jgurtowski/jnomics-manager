package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.io.FastqParser;
import edu.cshl.schatz.jnomics.manager.ManagerTask;
import edu.cshl.schatz.jnomics.ob.ReadCollectionWritable;
import edu.cshl.schatz.jnomics.ob.ReadWritable;
import edu.cshl.schatz.jnomics.util.FileUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Iterator;

/**
 * User: james
 */
public class PairedEndLoader implements ManagerTask{

    final static Log LOG = LogFactory.getLog(PairedEndLoader.class);

    /**
     *
     * @param in1 Input stream for first read pair file
     * @param in2 Input stream for second read pair file
     * @param hdfsOut Output path for paired-end file in hdfs
     * @param namenode set namenode for fs.default.name
     * @throws Exception
     */
    public void load(InputStream in1, InputStream in2, Path hdfsOut, String namenode) throws Exception {

        FastqParser parser1 = new FastqParser(in1);
        FastqParser parser2 = new FastqParser(in2);

        Configuration conf = getConf();
        if(namenode != null){
            LOG.info(namenode);
            conf.set("fs.default.name", namenode);
        }
        FileSystem fs = FileSystem.get(conf);

        if(fs.exists(hdfsOut)){
            throw new Exception("File already exists "+ hdfsOut);
        }

        NullWritable value = NullWritable.get();
        ReadCollectionWritable key = new ReadCollectionWritable();

        ReadWritable r1= new ReadWritable();
        ReadWritable r2 = new ReadWritable();
        key.addRead(r1);
        key.addRead(r2);
        Text keyName = new Text();
        key.setName(keyName);
        
        Iterator<FastqParser.FastqRecord> it1 = parser1.iterator();
        Iterator<FastqParser.FastqRecord> it2 = parser2.iterator();

        FastqParser.FastqRecord record1,record2;
        
        SequenceFile.Writer writer;
        if(conf.get("mapred.output.compress","").compareTo("true") == 0){
            String codec_str = conf.get("mapred.output.compression.codec","org.apache.hadoop.io.compress.GzipCodec");
            CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(Class.forName(codec_str), conf);
            writer = SequenceFile.createWriter(fs,conf,hdfsOut,key.getClass(),value.getClass(),
                    SequenceFile.CompressionType.BLOCK, codec);
        }else{//no compression
            writer = SequenceFile.createWriter(fs,conf,hdfsOut,key.getClass(),value.getClass());
        }

        LOG.info("Compressed with:" + writer.getCompressionCodec() );
        
        int counter = 1;
        while(it1.hasNext() && it2.hasNext()){
            record1 = it1.next();
            record2 = it2.next();
            keyName.set(Integer.toString(counter));
            r1.setAll(record1.getName(), record1.getSequence(), record1.getDescription(), record1.getQuality());
            r2.setAll(record2.getName(),record2.getSequence(),record2.getDescription(),record2.getQuality());
            writer.append(key,value);
            counter++;
            if(counter % 100000 == 0){
                LOG.info(counter);
                progress();
            }
        }
        parser1.close();
        parser2.close();
        writer.close();
    }

    public void load(InputStream in1, InputStream in2, Path out) throws Exception {
        load(in1,in2,out,null);
    }
    
    /**
     * Allows the task to report progess
     * Override to implement own progress hook
     */
    protected void progress(){
    }

    /**
     * Get a new configuration, or overload
     * @return Configuration
     */
    protected Configuration getConf(){
        return new Configuration();
    }
    

    public static void main(String[] args) throws Exception {

        if(args.length != 3 && args.length != 4 ){
            throw new Exception("Usage: " + PairedEndLoader.class + " <in.1.fq> <in.2.fq> output [hdfs://namenode:port/]");
        }

        File file1 = new File(args[0]);
        File file2 = new File(args[1]);
        Path out = new Path(args[2]+".pe");

        InputStream in1 = FileUtil.getInputStreamWrapperFromExtension(new FileInputStream(file1),
                FileUtil.getExtension(file1.getName()));
        InputStream in2 = FileUtil.getInputStreamWrapperFromExtension(new FileInputStream(file2),
                FileUtil.getExtension(file2.getName()));

        if(args.length == 3)
            new PairedEndLoader().load(in1,in2,out);
        else
            new PairedEndLoader().load(in1,in2,out,args[3]);
    }

    @Override
    public void runTask(String[] args) throws Exception{
        PairedEndLoader.main(args);
    }
}
