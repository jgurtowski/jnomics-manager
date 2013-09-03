package edu.cshl.schatz.jnomics.manager.server;

import java.net.URI;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.LoggerFactory;

import edu.cshl.schatz.jnomics.manager.api.JnomicsThriftException;

public class JnomicsFileSystem {
    
	private static final org.slf4j.Logger log = LoggerFactory.getLogger(JnomicsFileSystem.class);

	public static FileSystem getFileSystem(Properties properties, String username) 
												throws JnomicsThriftException {
        URI uri;
        String fsName = properties.getProperty("hdfs-default-name");
        try{
            uri = new URI(fsName);
        }catch(Exception e){
            log.error("probleming initializing hdfs");
            e.printStackTrace();
            throw new JnomicsThriftException(e.toString());
        }

        FileSystem fs = null;
        /**Check if the user has a home directory**/
        Path userHome = new Path("/user", username);
        try {
            fs = FileSystem.get(uri,new Configuration(), "hdfs");
            FileStatus[] stats = fs.listStatus(userHome);
            if(null == stats){ //create user's home directory
                fs.mkdirs(userHome,new FsPermission("755"));
                fs.setOwner(userHome,username,"kbase");
            }
            fs.close();
        } catch (Exception e) {
            log.error("Error Creating Filesystem");
            e.printStackTrace();
            throw new JnomicsThriftException(e.toString());
        }

        try{
            fs = FileSystem.get(uri, new Configuration(), username);
        } catch(Exception e){
            log.error("Problem creating filesystem");
            e.printStackTrace();
            throw new JnomicsThriftException(e.toString());
        }

        return fs;
    }

	public static void closeFileSystem(FileSystem fs) throws Exception{
		fs.close();
	}
}
		
