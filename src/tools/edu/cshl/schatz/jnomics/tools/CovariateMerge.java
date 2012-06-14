package edu.cshl.schatz.jnomics.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: james
 */
public class CovariateMerge {

    public static void main(String []args) throws IOException {

        if(!(2==args.length)){
            System.out.println("CovariateMerge <incov_dir> <out.covar>");
            System.exit(0);
        }

        Configuration conf = new Configuration();

        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        
        FileSystem fs = FileSystem.get(conf);

        /** Put covariate data into map**/
        Map<String,int[]> data = new HashMap<String,int[]>();
        String header = null;
        for(FileStatus stat : fs.listStatus(in)){
            if(stat.getPath().getName().endsWith(".covar")){
                System.out.println("Processing: "+ stat.getPath());
                FSDataInputStream stream = fs.open(stat.getPath());
                BufferedReader reader = new BufferedReader(new InputStreamReader((InputStream)stream));
                String line;
                while((line = reader.readLine()).startsWith("#"))
                    continue;
                header = line;
                String []arr;
                String key;
                while(!(line = reader.readLine()).startsWith("EOF")){
                    arr = line.split(",");
                    key = arr[0]+","+arr[1]+","+arr[2]+","+arr[3];
                    int []t;
                    if(data.containsKey(key)){
                        t = data.get(key);
                        t[0] += Integer.parseInt(arr[4]);
                        t[1] += Integer.parseInt(arr[5]);
                    }else{
                        data.put(key,new int[]{Integer.parseInt(arr[4]),Integer.parseInt(arr[5])});
                    }
                }
                reader.close();
            }
        }
        
        System.out.println("Writing output file: "+ out);
        /**Write to output file **/
        FSDataOutputStream outStream = fs.create(out);
        PrintWriter writer = new PrintWriter((OutputStream)outStream);
        writer.println(header);
        StringBuilder builder = new StringBuilder();
        for(Map.Entry<String,int[]> entry:data.entrySet()){
            int []vals = entry.getValue();
            builder.append(entry.getKey());
            builder.append(",");
            builder.append(vals[0]);
            builder.append(",");
            builder.append(vals[1]);
            builder.append(",");
            builder.append(Math.round(-10 * Math.log10((float)vals[1]/vals[0])));
            writer.println(builder);
            builder.setLength(0);
        }
        writer.println("EOF");
        writer.close();
    }
}
