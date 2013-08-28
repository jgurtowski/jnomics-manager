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

/**
 * User: james
 */
public class ShockWrite extends FSBase{

    @Flag(shortForm = "-h", longForm = "--help")
    public boolean help;

    @Parameter(shortForm = "-hdfs_file", longForm = "--hdfsFilename", description = "HDFS Filename")
    public String hdfs_file;
    
    @Parameter(shortForm = "-hdfs_path", longForm = "--hdfspath", description = "Path on the Cluster")
    public String hdfs_path;
    
    
    @Override
    public void handle(List<String> remainingArgs, Properties properties) throws Exception {
        super.handle(remainingArgs, properties);
        
        if(help || hdfs_file == null || hdfs_path == null){
        	System.out.println("shock_write -hdfs_file=<Name> -hdfs_path=<Path>");
        }else{
        	if(client.ShockWrite(hdfs_file, hdfs_path, auth)){
        		System.out.println("Moved : " + hdfs_file + " from the Cluster to Shock Client");
        		
        	}
        	else {
        		System.out.println("Failed\n");
        	}
        }
        
    }
}
