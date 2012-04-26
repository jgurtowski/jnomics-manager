package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.manager.ManagerTask;
import edu.cshl.schatz.jnomics.ob.writable.StringArrayWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;


/**
 * User: james
 */
public class ManifestLoader implements ManagerTask {

    static final Log LOG = LogFactory.getLog(ManifestLoader.class);

   
    public static void load(InputStream in1, Path output) throws Exception {

        BufferedReader reader = new BufferedReader(new InputStreamReader(in1));;

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        StringArrayWritable arr;
        SequenceFile.Writer writer = SequenceFile.createWriter(fs,conf,output,
                IntWritable.class,StringArrayWritable.class);

        String line;
        IntWritable iw = new IntWritable();
        int count = 0;
        while((line = reader.readLine()) != null){
            String a[] = line.split("\t");
            arr = new StringArrayWritable(a);
            iw.set(count);
            writer.append(iw, arr);
            count++;
        }
        writer.close();
    }

    public static void main(String[] args) throws Exception {

        if(args.length != 2){
            throw new Exception("Usage: " + ManifestLoader.class + " manifest.txt outputPath");
        }

        load(new FileInputStream(args[0]),new Path(args[1]));
    }

    @Override
    public void runTask(String[] args) throws Exception{
        ManifestLoader.main(args);
    }
}
