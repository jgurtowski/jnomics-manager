package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.ob.ReadCollectionWritable;
import edu.cshl.schatz.jnomics.ob.ReadWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;

import java.io.*;

/**
 * User: james
 */
public class PairedEndLoader {

    public static void main(String[] args) throws IOException{

        if(args.length != 3){
            System.err.println("Usage: " + PairedEndLoader.class + " <in.1.fq> <in.2.fq> output");
            System.exit(1);
        }

        Path in1 = new Path(args[0]);
        Path in2 = new Path(args[1]);
        Path out = new Path(args[2]+".pe");
        
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        if(fs.exists(out)){
            System.err.println("Path: " + out  + " exists already");
            System.exit(1);
        }

        BufferedReader fq1 = new BufferedReader(new FileReader(in1.toString()));
        BufferedReader fq2 = new BufferedReader(new FileReader(in2.toString()));

        String[] items = new String[8];

        long count =0;
        
        ReadCollectionWritable key = new ReadCollectionWritable();
        NullWritable value = NullWritable.get();

        ReadWritable r1 = new ReadWritable();
        ReadWritable r2 = new ReadWritable();
        key.addRead(r1); key.addRead(r2);

        SequenceFile.Writer writer = SequenceFile.createWriter(fs,conf,out,key.getClass(),value.getClass());
        fq_files:while(true){
            for(int i=0; i<4;i++){
                items[i] = fq1.readLine();
                items[i+4] = fq2.readLine();
                if(items[i] == null || items[i+4] == null)
                    break fq_files;
            }

            if (count % 100000 == 0)
                System.out.println(count);

            int idx = Math.max(items[0].indexOf("/"), items[0].indexOf("#"));
            if(idx > 2){
                key.getName().set(items[0].substring(0, idx));
            }else{
                key.getName().set(items[0]);
            }

            r1.getName().set(items[0]);
            r1.getSequence().set(items[1]);
            r1.getDescription().set(items[2]);
            r1.getQuality().set(items[3]);
            r2.getName().set(items[4]);
            r2.getSequence().set(items[5]);
            r2.getDescription().set(items[6]);
            r2.getQuality().set(items[7]);

            writer.append(key,value);
            
            count++;
        }
        
        writer.close();

        fq1.close();
        fq2.close();
        
        System.out.println("Wrote " + count + " Pairs, totaling "+ fs.getFileStatus(out).getLen() + " bytes");
        
    }

}
