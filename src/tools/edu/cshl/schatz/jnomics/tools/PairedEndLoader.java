package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.io.FastqParser;
import edu.cshl.schatz.jnomics.manager.ManagerTask;
import edu.cshl.schatz.jnomics.ob.ReadCollectionWritable;
import edu.cshl.schatz.jnomics.ob.ReadWritable;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;

import java.io.*;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;

/**
 * User: james
 */
public class PairedEndLoader implements ManagerTask {

    static final Log LOG = LogFactory.getLog(PairedEndLoader.class);

    /**
     *
     * @param in1 Input stream for first read pair file
     * @param in2 Input stream for second read pair file
     * @param hdfsOut Output path for paired-end file in hdfs
     * @param namenode set namenode for fs.default.name
     * @throws Exception
     */
    public static void load(InputStream in1, InputStream in2, Path hdfsOut, String namenode) throws Exception {

        FastqParser parser1 = new FastqParser(in1);
        FastqParser parser2 = new FastqParser(in2);

        Configuration conf = new Configuration();
        if(namenode != null){
            System.out.println(namenode);
            conf.set("fs.default.name",namenode);
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
        SequenceFile.Writer writer = SequenceFile.createWriter(fs,conf,hdfsOut,key.getClass(),value.getClass(),
                SequenceFile.CompressionType.BLOCK, new GzipCodec());
        System.out.println("Compressed with:" + writer.getCompressionCodec() );
        
        int counter = 1;
        while(it1.hasNext() && it2.hasNext()){
            record1 = it1.next();
            record2 = it2.next();
            keyName.set(Integer.toString(counter));
            r1.setAll(record1.getName(), record1.getSequence(), record1.getDescription(), record1.getQuality());
            r2.setAll(record2.getName(),record2.getSequence(),record2.getDescription(),record2.getQuality());
            writer.append(key,value);
            counter++;
            if(counter % 100000 == 0)
                System.err.println(counter);

        }
        parser1.close();
        parser2.close();
        writer.close();
    }

    public static void load(InputStream in1, InputStream in2, Path hdfsOut) throws Exception {
        load(in1,in2,hdfsOut,null);
    }

    public static void main(String[] args) throws Exception {

        if(args.length != 3 && args.length != 4 ){
            throw new Exception("Usage: " + PairedEndLoader.class + " <in.1.fq> <in.2.fq> output [hdfs://namenode:port/]");
        }

        File file1 = new File(args[0]);
        File file2 = new File(args[1]);
        Path out = new Path(args[2]+".pe");

        InputStream in1 = new FileInputStream(file1);
        InputStream in2 = new FileInputStream(file2);

        if(file1.getName().endsWith(".gz")){
            in1 = new GZIPInputStream(in1);
        }else if(file1.getName().endsWith(".bz2"))
            in1 = new BZip2CompressorInputStream(in1);
        if(file2.getName().endsWith(".gz"))
            in2 = new GZIPInputStream(in2);
        else if(file2.getName().endsWith(".bz2")){
            in2 = new BZip2CompressorInputStream(in2);
        }

        if(args.length == 3)
            load(in1,in2,out);
        else
            load(in1,in2,out,args[3]);
    }

    @Override
    public void runTask(String[] args) throws Exception{
        PairedEndLoader.main(args);
    }
}
