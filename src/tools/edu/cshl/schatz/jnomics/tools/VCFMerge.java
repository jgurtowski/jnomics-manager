package edu.cshl.schatz.jnomics.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * User: james
 * Merges VCF files in hdfs
 */
public class VCFMerge {

    public static void main(String []args) throws IOException {

        if(2 != args.length){
            System.out.println("VCFMerge <indir_vcfs> <out.vcf>");
            System.exit(-1);
        }        
        Path in = new Path(args[0]);
        Path out = new Path(args[2]);
        
        FileSystem fs = FileSystem.get(new Configuration());
        FileStatus[] stats = fs.listStatus(in);
        Path cur;
        for(FileStatus fileStat: stats){
            cur = fileStat.getPath();
            if(cur.getName().endsWith(".vcf")){
                final InputStream stream = fs.open(cur);
                BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
                String line;
                StringBuilder header = new StringBuilder();
                List<String> headerOrder = new ArrayList<String>();
                while(null != (line = reader.readLine())){
                    if(line.startsWith("#")){
                        if(line.startsWith("##contig=")){
                            headerOrder.add(line.substring(13, line.indexOf(",length"))); //extract ref name
                        }
                        header.append(line);
                    }else{
                        break;
                    }
                }
            }
        }
    }
}
