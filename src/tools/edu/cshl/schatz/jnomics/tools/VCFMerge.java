package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.ob.SAMRecordWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

    private static final Log log = LogFactory.getLog(VCFMerge.class);

    public static void merge(Path in, Path alignments, Path out, Configuration conf) throws Exception{

        FileSystem fs = FileSystem.get(conf);

        /** Get reference order from header of alignments **/
        List<String> alignmentOrder = new ArrayList<String>();
        boolean found = false;
        SAMRecordWritable samRecordWritable = new SAMRecordWritable();
        for(FileStatus stat: fs.listStatus(alignments)){
            if(stat.getPath().getName().startsWith("part-m") || stat.getPath().getName().startsWith("part-r")){

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


        log.info("Found References : " + alignmentOrder);

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
                log.info("Merged: " + vcfFile);
            }
        }
        outStream.close();
        log.info("Done!");
        log.info("Created merged VCF: " + out);
    }


    public static void main(String []args) throws Exception {

        if(3 != args.length){
            throw new Exception("VCFMerge <indir_vcfs> <indir_alignments> <out.vcf>");
        }
        Path in = new Path(args[0]);
        Path alignments = new Path(args[1]);
        Path out = new Path(args[2]);

        Configuration conf = new Configuration();
        
        merge(in,alignments,out,conf);
    }
}
