package edu.cshl.schatz.jnomics.manager.client.compute;

import edu.cshl.schatz.jnomics.manager.api.JnomicsThriftHandle;
import edu.cshl.schatz.jnomics.manager.api.JnomicsThriftJobID;
import edu.cshl.schatz.jnomics.manager.client.Utility;
import edu.cshl.schatz.jnomics.manager.client.ann.Flag;
import edu.cshl.schatz.jnomics.manager.client.ann.Parameter;
import edu.cshl.schatz.jnomics.manager.client.fs.FSBase;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * User: james
 */
public class ShockBatchWrite extends ComputeBase{

    @Flag(shortForm = "-h", longForm = "--help")
    public boolean help;

    @Parameter(shortForm = "-hdfs_files", longForm = "--hdfsFilenames", description = "HDFS Filenames")
    public String hdfs_files;
    
    @Parameter(shortForm = "-hdfs_Output", longForm = "--hdfsOutput", description = "HDFS Output path")
    public String hdfs_Output;
    
//    @Parameter(shortForm = "-hdfs_path", longForm = "--hdfspath", description = "Path on the Cluster")
//    public String hdfs_path;
    
    
    @Override
    public void handle(List<String> remainingArgs, Properties properties) throws Exception {
        super.handle(remainingArgs, properties);
        
        if(help || hdfs_files == null || hdfs_Output == null ){
        	System.out.println("shock_batch_write -hdfs_files=<Name1,Name2,Name3,Name4> -hdfs_Output=<hdfs Output>");
        }else{
        	List<String> items = Arrays.asList(hdfs_files.split("\\s*,\\s*"));
        	for(String item : items){
        	System.out.println(item);
        	}
        	try{
        	System.out.println("Inside trycat ");
        	JnomicsThriftJobID jobID = client.ShockBatchWrite(items, hdfs_Output, auth);
        	System.out.println("Submitted Job: " + jobID.getJob_id());
        	}catch(Exception e){
        		//System.err.println(e.toString());
        		throw new Exception(e.toString());
        	}
            return;		
      	}
        System.out.println(Utility.helpFromParameters(this.getClass()));	
        }
   }

