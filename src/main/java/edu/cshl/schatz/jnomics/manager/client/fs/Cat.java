package edu.cshl.schatz.jnomics.manager.client.fs;

import edu.cshl.schatz.jnomics.manager.api.JnomicsThriftFileStatus;
import edu.cshl.schatz.jnomics.manager.api.JnomicsThriftHandle;
import edu.cshl.schatz.jnomics.manager.client.ann.Flag;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;


/**
 *User: James
 */


public class Cat extends FSBase{

    @Flag(shortForm = "-h", longForm = "--help")
    public boolean help;


    @Override
    public void handle(List<String> remainingArgs, Properties properties) throws Exception {
	super.handle(remainingArgs, properties);
	if(remainingArgs.size() < 1 || help){
            System.out.println("-cat <hdfs_file> ");
            return;
	}else{
	    String remoteFile = remainingArgs.get(0);
            List<JnomicsThriftFileStatus> stats  = client.listStatus(remoteFile, auth);
            if(1 != stats.size())
                throw new Exception("Remote file does not exist or is not a single file");
	    long remoteLen = stats.get(0).getLength();
	    JnomicsThriftHandle handle = client.open(remoteFile, auth);
	    
	    ByteBuffer buffer;
            while((buffer = client.read(handle,auth)).remaining() > 0){
                System.out.write(buffer.array());
            }
            
            client.close(handle,auth);
	}
    }
}
