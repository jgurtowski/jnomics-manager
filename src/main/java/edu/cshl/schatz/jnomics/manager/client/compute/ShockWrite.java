package edu.cshl.schatz.jnomics.manager.client.compute;

import edu.cshl.schatz.jnomics.manager.api.JnomicsThriftHandle;
import edu.cshl.schatz.jnomics.manager.api.JnomicsThriftJobID;
import edu.cshl.schatz.jnomics.manager.client.ann.Flag;
import edu.cshl.schatz.jnomics.manager.client.ann.Parameter;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.Path;

/**
 * User: james
 */
public class ShockWrite extends ComputeBase{

    @Flag(shortForm = "-h", longForm = "--help")
    public boolean help;

    @Parameter(shortForm = "-hdfs_file", longForm = "--hdfsFilename", description = "HDFS Filename")
    public String hdfs_file;
    
    @Override
    public void handle(List<String> remainingArgs, Properties properties) throws Exception {
        super.handle(remainingArgs, properties);
        if(help || hdfs_file == null){
        	System.out.println("shock_write -hdfs_file=<Name>" );
        }else{
        	if(!fsclient.checkFileStatus(hdfs_file, auth)){
        		throw new Exception("ERROR: " + hdfs_file + "doesn't exist");
        	}

        	String filename = new Path(hdfs_file).getName();
        	System.out.println("filename is " + filename);	
        	JnomicsThriftJobID jobID = client.ShockWrite(filename,hdfs_file,auth);
        	System.out.println("Submitted : " + jobID.getJob_id());	

        }
        
    }
}
