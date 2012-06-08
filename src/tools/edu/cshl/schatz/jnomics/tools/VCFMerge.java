package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.ob.SAMRecordWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.SequenceFile;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;

/**
 * User: james
 * Merges VCF files in hdfs
 */
public class VCFMerge {

    public static void main(String []args) throws Exception {

        if(3 != args.length){
            System.out.println("VCFMerge <indir_vcfs> <indir_alignments> <out.vcf>");
            System.exit(-1);
        }        
        Path in = new Path(args[0]);
        Path alignments = new Path(args[1]);
        Path out = new Path(args[2]);

        Configuration conf = new Configuration();
        
        FileSystem fs = FileSystem.get(conf);

        /** Get reference order from header of alignments **/
        List<String> alignmentOrder = new ArrayList<String>();
        boolean found = false;
        SAMRecordWritable samRecordWritable = new SAMRecordWritable();
        for(FileStatus stat: fs.listStatus(alignments)){
            if(stat.getPath().getName().startsWith("part-m")){

                SequenceFile.Reader reader = new SequenceFile.Reader(fs,stat.getPath(),conf);
                if(reader.getKeyClass() != SAMRecordWritable.class)
                    throw new Exception("Expected SAMRecordWritable for sequence file");
                reader.next(samRecordWritable);
                for(String line: samRecordWritable.getTextHeader().toString().split("\n")){
                    if(line.startsWith("@SQ")){
                        alignmentOrder.add(line.split("\t")[1].split(":")[1]);
                    }
                }
                if(alignmentOrder.size() < 1)
                    throw new Exception("Bad Header in Alignments could not find @SQ");
                reader.close();
                found = true;
                break;
            }
        }
        if(!found)
            throw new Exception("Alignment directory does not look good");

        
        System.out.println("Found References : " + alignmentOrder);
        
        /** Take inventory of VCF files **/
        FileStatus[] stats = fs.listStatus(in);
        Path cur;
        Map fileMap = new HashMap<String,ArrayList<Integer>>();
        for(FileStatus fileStat: stats){
            cur = fileStat.getPath();
            String name = cur.getName();
            if(name.endsWith(".vcf")){
                String[] arr = name.split("-");
                if(2 != arr.length){
                    throw new Exception("VCF files not named properly. looking for format <ref>-<bin>.vcf");
                }
                if(!fileMap.containsKey(arr[0]))
                    fileMap.put(arr[0], new ArrayList<Integer>());
                ((ArrayList<Integer>)fileMap.get(arr[0])).add(Integer.parseInt(arr[1].substring(0, arr[1].indexOf(".vcf"))));
            }
        }

        /** Sort the VCF files **/
        for(Object l: fileMap.values()){
            Collections.sort((List<String>) l);
        }

        /** Join VCF files **/
        FSDataOutputStream outStream = fs.create(out);
        boolean firstFile = true;
        for(String ref: alignmentOrder){
            if(!fileMap.containsKey(ref))
                throw new Exception("VCF file names (reference names) do not agree with alignment headers");
            for(Integer filePart : (List<Integer>)fileMap.get(ref)){
                Path vcfFile = new Path(in + "/" + ref + "-" + filePart + ".vcf");
                FSDataInputStream inStream = fs.open(vcfFile);
                BufferedReader bReader = new BufferedReader(new InputStreamReader(inStream));
                String line;
                while(null != (line = bReader.readLine())){
                    if(!firstFile && line.startsWith("#"))
                        continue;
                    outStream.write(line.getBytes());
                    outStream.write("\n".getBytes());
                }
                firstFile = false;
                inStream.close();
                System.out.println("Merged: " + vcfFile);
            }
        }
        outStream.close();
        System.out.println("Done!");
        System.out.println("Created merged VCF: " + out);
    }
}
