package edu.cshl.schatz.jnomics.manager.client.fs;

import edu.cshl.schatz.jnomics.manager.api.JnomicsThriftHandle;
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
public class ShockWrite extends FSBase{

    @Flag(shortForm = "-h", longForm = "--help")
    public boolean help;

    @Parameter(shortForm = "-hdfs_file", longForm = "--hdfsFilename", description = "HDFS Filename")
    public String hdfs_file;
//    
//    @Parameter(shortForm = "-hdfs_path", longForm = "--hdfspath", description = "Path on the Cluster")
//    public String hdfs_path;
    
    
    @Override
    public void handle(List<String> remainingArgs, Properties properties) throws Exception {
        super.handle(remainingArgs, properties);
        String nodeid = null;
        if(help || hdfs_file == null){
        	System.out.println("shock_write -hdfs_file=<Name>" );
        }else{
        	if(!client.checkFileStatus(hdfs_file, auth)){
        		throw new Exception("ERROR: " + hdfs_file + "doesn't exist");
        	}
        	JnomicsThriftHandle handle = client.open(hdfs_file, auth);
        	
        	String filename = new Path(hdfs_file).getName();
//            InputStream localStream = new FileInputStream(inFile);
        	nodeid = client.ShockWrite3(handle, filename,auth);
        	System.out.println("Copied : " + hdfs_file + " to Shock Client : " + nodeid);	
        	
        	
        	
        	client.close(handle,auth);	
        }
        
    }
}
