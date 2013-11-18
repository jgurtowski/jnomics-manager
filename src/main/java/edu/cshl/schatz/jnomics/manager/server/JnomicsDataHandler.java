package edu.cshl.schatz.jnomics.manager.server;

import edu.cshl.schatz.jnomics.manager.api.*;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsJobBuilder;
import edu.cshl.schatz.jnomics.tools.SELoaderMap;
import edu.cshl.schatz.jnomics.tools.ShockLoad;
import edu.cshl.schatz.jnomics.manager.server.NLineInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.thrift.TException;
import org.slf4j.LoggerFactory;

import us.kbase.shock.client.BasicShockClient;
import us.kbase.shock.client.ShockNode;
import us.kbase.shock.client.ShockNodeId;
import us.kbase.auth.AuthService;
import us.kbase.auth.AuthToken;
import us.kbase.auth.AuthUser;
import us.kbase.auth.TokenExpiredException;
import us.kbase.shock.client.exceptions.InvalidShockUrlException;
import us.kbase.shock.client.exceptions.ShockHttpException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.net.MalformedURLException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * User: james
 * Modeled after HadoopThriftfs
 */

public class JnomicsDataHandler implements JnomicsData.Iface {

    private final org.slf4j.Logger log = LoggerFactory.getLogger(JnomicsDataHandler.class);
    private final Map<UUID, JnomicsFsHandle> handleMap = Collections.synchronizedMap(new HashMap<UUID, JnomicsFsHandle>());
    private Properties properties;

    //private static final int SHOCK_BUFFER_SIZE = 10000000;
    
    private JnomicsServiceAuthentication authenticator;

       
    private final ThreadLocal bufferCache = new ThreadLocal(){
        @Override
        protected Object initialValue() {
            return new byte[2000000];
        }
    };
    
    

    public JnomicsDataHandler(Properties props){
        properties = props;
        authenticator = new JnomicsServiceAuthentication();
    }

    private UUID getUniqueUUID(){
        UUID b;
        while(handleMap.containsKey(b = UUID.randomUUID())){}
        return b;
    }

     private FileSystem getFileSystem(String username) throws JnomicsThriftException {
    	 return JnomicsFileSystem.getFileSystem(properties, username);
     }
   
     private void  closeFileSystem(FileSystem fs) throws JnomicsThriftException{
    	 try{
    	   JnomicsFileSystem.closeFileSystem(fs);
        }catch(Exception e){
           throw new JnomicsThriftException(e.toString());
        }
    }
    
    public Map<UUID, JnomicsFsHandle> getHandleMap(){
        return handleMap;
    }
    
    @Override
    public JnomicsThriftHandle create(String path, Authentication auth) throws TException, JnomicsThriftException {
        String username;
        if(null == (username = authenticator.authenticate(auth))){
            throw new JnomicsThriftException("Permission Denied");
        }
        log.info("Creating file: " + path + " for user: "+ username);

        FileSystem fs = getFileSystem(username);

        FSDataOutputStream stream = null;
        try {
            stream = fs.create(new Path(path));
        } catch (IOException e) {
            log.error("Problem creating file " + path);
            e.printStackTrace();
            throw new JnomicsThriftException(e.toString());
        }
        
        UUID nxtUUID = getUniqueUUID();
        handleMap.put(nxtUUID,new JnomicsFsHandle(fs,stream));
        
        return new JnomicsThriftHandle(nxtUUID.toString());
    }

    @Override
    public JnomicsThriftHandle open(String path, Authentication auth) throws JnomicsThriftException, TException {
        String username;
        if(null == (username = authenticator.authenticate(auth))){
            throw new JnomicsThriftException("Permission Denied");
        }

        log.info("Opening file: " + path + " for user: " + username);
        
        FileSystem fs = getFileSystem(username);
        FSDataInputStream stream = null;
        try{
            stream = fs.open(new Path(path));
        }catch(Exception e){
            log.error("Problem opening file: " + path);
            e.printStackTrace();
            throw new JnomicsThriftException(e.toString());
        }

        UUID nxtUUID = getUniqueUUID();
        handleMap.put(nxtUUID,new JnomicsFsHandle(fs,stream));
        return new JnomicsThriftHandle(nxtUUID.toString());
    }


    @Override
    public void write(JnomicsThriftHandle handle, ByteBuffer data, Authentication auth) throws TException, JnomicsThriftException {
        JnomicsFsHandle jhandle = handleMap.get(UUID.fromString(handle.getUuid()));
        try {
            jhandle.getOutStream().write(data.array());
        } catch (IOException e) {
            log.error("Problem writing to file");
            e.printStackTrace();
            throw new JnomicsThriftException(e.toString());
        }
        jhandle.updateLastUsed();
    }
    
    @Override
    public ByteBuffer read(JnomicsThriftHandle handle, Authentication auth) throws TException, JnomicsThriftException {
        JnomicsFsHandle jhandle = handleMap.get(UUID.fromString(handle.getUuid()));

        byte[] buf = (byte[]) bufferCache.get();
        int bytesRead;
        try{
            bytesRead = jhandle.getInStream().read(buf);
        } catch (IOException e) {
            throw new JnomicsThriftException(e.toString());
        }
        jhandle.updateLastUsed();
        if(-1 == bytesRead)
            return ByteBuffer.allocate(0);
        return ByteBuffer.wrap(buf,0,bytesRead);
    }

    @Override
    public boolean ShockRead(String shockNodeID, String hdfsPathDest, Authentication auth) throws TException , JnomicsThriftException{	
    	String username;
    	if(null == (username = authenticator.authenticate(auth))){
    		throw new JnomicsThriftException("Permission Denied");
    	}
    	AuthToken oauth;
    	FileSystem fs = null;
//    	OutputStream out = null;
    	FSDataOutputStream fsout = null;
    	URL shockurl = null;
	BasicShockClient base;
    	log.info("Inside shock client read" );
    	try{
    		   		
    		oauth = new AuthToken(auth.token);
    		log.info("auth token is " + oauth);
    		shockurl = new URL(properties.getProperty("shock-url"));
    		log.info("shock-url is " + shockurl);
    		base = new BasicShockClient(shockurl,oauth);
    		log.info("calling the getFile funstion ");
    		fs = getFileSystem(username);
    		fsout = fs.create(new Path(hdfsPathDest));
//    		out = fs.create(new Path(hdfsPathDest));
    		base.getFile(new ShockNodeId(shockNodeID),fsout);
		    
	} catch (Exception e) {
    		e.printStackTrace();
    		throw new JnomicsThriftException(e.toString());
    	}finally{
    		closeFileSystem(fs);
    		try {
				fsout.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

    	}
    	return true;
    }

    @Override
    public boolean ShockWrite(JnomicsThriftHandle handle,String filename, Authentication auth) throws TException , JnomicsThriftException{	
    	String username;
    	// 	   	JnomicsFsHandle jhandle = handleMap.get(UUID.fromString(handle.getUuid()));
    	
    	if(null == (username = authenticator.authenticate(auth))){
    	    		throw new JnomicsThriftException("Permission Denied");
    	}
    	ByteBuffer buffer;
    	byte[] bytearr;
    	long totalTransfer = 0;
    	URL shockurl = null;	
    	String file = new Path(filename).getName();
    	log.info("Inside shock write funtion");
    	BasicShockClient base;
    	int cnt= 0;
    	//    	FileSystem fs = null;
    	//    	FSDataInputStream inStream = null;
    	try{
    		shockurl = new URL(properties.getProperty("shock-url"));
    		log.info("Shock url is " + shockurl);
    		base = new BasicShockClient(shockurl);
    		log.info("base is " + base.getShockUrl());
//    		while((buffer = read(handle,auth)).remaining() > 0){
//    			log.info("Buffer read is " +  totalTransfer + " cnt is " + cnt);
//    			totalTransfer += buffer.remaining();
//    			log.info("buffer array length " + buffer.array().length);
////    			ShockNode sn = base.addNode(buffer.array(),filename);
////    			log.info("Node id is "+ sn.getId().toString());
//    			cnt++;
//    		}
    	}catch(Exception e){
    		log.error(e.toString());
    		throw new JnomicsThriftException(e.getMessage());

    	}
    	//    	List<JnomicsThriftFileStatus> stats  = listStatus(hdfsPath, auth);
    	//    	try { 
    	//    		fs = getFileSystem(username);
    	//    		inStream = fs.open(new Path(hdfsPath));
    	//    		int  Length = (int)(fs.getLength(new Path(hdfsPath)));
    	//    		byte[] buf = new byte[Length];
    	//    		shockurl = new URL(properties.getProperty("shock-url"));
    	//    		base = new BasicShockClient(shockurl);
    	//    		int i = 0;
    	//    		int total = 0;
    	//    		inStream.read(buf);
    	//    		while(-1 != (i = inStream.read(buf))){
    	//    			total += i;
    	//    			//System.out.print("\r"+total+"/"+Length+" " + ((float)total)/Length * 100 + "%");    
    	//    		}

    	//    			ShockNode sn = base.addNode(buf, filename);
    	//    			log.info("Node id is "+ sn.getId().toString());
    	//    		}	
    	//    		catch(Exception e){
    	//    			log.error(e.toString());
    	//    			throw new IOException(e.getMessage());
    	//    		}
    	//    		inStream.close();
    	//    	}catch (Exception e) {
    	//    		log.error(e.toString());
    	//    		
    	//    	}finally{
    	//    		closeFileSystem(fs);
    	//
    	//    	}

    	return true;
    }
    
    @Override
    public boolean ShockWrite2(String filename, String hdfsPath, Authentication auth) throws TException , JnomicsThriftException{	
    	String username;
    	if(null == (username = authenticator.authenticate(auth))){
    		throw new JnomicsThriftException("Permission Denied");
    	}
    	log.info("Opening file: " + hdfsPath + " for user: " + username);
    	BasicShockClient base;
    	FileSystem fs = null;
    	AuthToken oauth; 
    	FSDataInputStream inStream = null;
    	URL shockurl = null;
    	List<JnomicsThriftFileStatus> stats  = listStatus(hdfsPath, auth);
    	try { 
    		oauth =  new AuthToken(auth.token);
    		shockurl = new URL(properties.getProperty("shock-url"));
    		fs = getFileSystem(username);
    		inStream = fs.open(new Path(hdfsPath));
//    		long  Length = (long)(fs.getLength(new Path(hdfsPath)));
    		byte[] buf = (byte[]) bufferCache.get();
    		
    		base = new BasicShockClient(shockurl,oauth);
    		int i = 0;
    		int total = 0;
//    		inStream.read(buf);
    		while(-1 != (i = inStream.read(buf))){
    			total += i;
    			//System.out.print("\r"+total+"/"+Length+" " + ((float)total)/Length * 100 + "%");    
    		}
    		try { 	
    			log.info(" Calling addNode " );
    			ShockNode sn = base.addNode(inStream, filename);
    		}	
    		catch(Exception e){
    			log.error(e.toString());
    			throw new IOException(e.getMessage());
    		}
    		inStream.close();
    	}catch (Exception e) {
    		log.error(e.toString());
    		
    	}finally{
    		closeFileSystem(fs);

    	}
    	return true;
    }
    
    @Override
    public String ShockWrite3(JnomicsThriftHandle handle, String filename, Authentication auth) throws TException , JnomicsThriftException{	
    	log.info(" Entered ShockWrite3 " );
    	JnomicsFsHandle jhandle = handleMap.get(UUID.fromString(handle.getUuid()));
    	String username;
    	
    	if(null == (username = authenticator.authenticate(auth))){
    		throw new JnomicsThriftException("Permission Denied");
    	}
    	InputStream input = null;
//    	log.info("Opening file: " + hdfsPath + " for user: " + username);
//    	FileSystem fs = null;
//   	FSDataInputStream inStream = null;
//    	List<JnomicsThriftFileStatus> stats  = listStatus(hdfsPath, auth);

    	BasicShockClient base;
    	AuthToken oauth; 
    	URL shockurl = null;
    	String id = null ;
    	try { 
    		log.info("try block");
    		input = jhandle.getInStream();
    		log.info("input stream is got ");
    		oauth =  new AuthToken(auth.token);
    		shockurl = new URL(properties.getProperty("shock-url"));
//    		fs = getFileSystem(username);
//    		inStream = fs.open(new Path(hdfsPath));
//    		long  Length = (long)(fs.getLength(new Path(hdfsPath)));
//    		byte[] buf = (byte[]) bufferCache.get();
//    		int i = 0;
//    		int total = 0;
//    		inStream.read(buf);
//    		while(-1 != (i = inStream.read(buf))){
//    			total += i;
    			//System.out.print("\r"+total+"/"+Length+" " + ((float)total)/Length * 100 + "%");    
//    		}
    		base = new BasicShockClient(shockurl,oauth);
    		log.info(" Calling addNode " );
    		ShockNode sn = base.addNode(input,filename );
    		id = sn.getId().toString();
    	}catch (Exception e) {
    		log.error(e.toString());
    		throw new JnomicsThriftException("ERROR Writing File to Shock" + e.toString());
    	}
    	return id;
    }
    @Override
    public void close(JnomicsThriftHandle handle, Authentication auth) throws TException, JnomicsThriftException {
        UUID u = UUID.fromString(handle.getUuid());
        JnomicsFsHandle jhandle = handleMap.get(u);


        if(jhandle.getOutStream() != null){
            try {
                jhandle.getOutStream().close();
            } catch (IOException e) {
                log.error("Problem closing file");
                e.printStackTrace();
                throw new JnomicsThriftException(e.toString());
            }
        }else if(jhandle.getInStream() != null){
            try {
                jhandle.getInStream().close();
            } catch (IOException e) {
                log.error("Problem closing file");
                e.printStackTrace();
                throw new JnomicsThriftException(e.toString());
            }
        }

        try {
            closeFileSystem(jhandle.getFileSystem());
        }finally{
            handleMap.remove(u);
        }
    }
    
    @Override
    public List<String> listShockStatus(String path,
    		Authentication auth) throws JnomicsThriftException, TException {
    	/***Requires changes to WorkspaceDeluxe */
//
//	  String username;
//    	if(null == (username = authenticator.authenticate(auth))){
//    		throw new JnomicsThriftException("Permission Denied");
//    	}
//    	AuthToken oauth;
//    	URL shockurl = null;
//    	try { 
//    		shockurl = new URL(properties.getProperty("shock-url"));
//    		oauth =  new AuthToken(auth.token);
//    		log.info("auth token is " + oauth);
//    		BasicShockClient base = new BasicShockClient(shockurl,oauth);
//    		System.out.println(base.getShockUrl());
//    		Map<String,Object> filelist = base.getFileList();
//    		List<String> shockfiles =  new ArrayList<String>();
//    		for( Map<String,Object> file: (ArrayList<Map<String,Object>>) filelist.get("data")){
//    			String fid = file.get("id").toString();
//    			if( !fid.equals(null) ){
//    				LinkedHashMap<String,Object> name = (LinkedHashMap<String,Object>)file.get("file");
//    				if(!name.get("name").equals("")){
//    					shockfiles.add(fid + "\t" + name.get("name") + "\t" + name.get("size"));
//    				}	
//
//    			}
//
//    		}
//    		return shockfiles;
//    	}
//    	catch(Exception  e){
//    		log.error("Could not open Shock");
//    		throw new JnomicsThriftException(e.getMessage());
//		}
	 return null;
    }

	@Override
    public List<JnomicsThriftFileStatus> listStatus(String path, Authentication auth) throws TException, JnomicsThriftException {
        String username;
        if(null == (username = authenticator.authenticate(auth))){
            throw new JnomicsThriftException("Permission Denied");
        }
        
        log.info("Getting file status of "+ path + " for user "+ username);
        
        FileSystem fs = getFileSystem(username);
        FileStatus[] stats;
        try{
            stats = fs.listStatus(new Path(path));
        }catch(Exception e){
            log.error("Could not open filesystem in listStatus");
            e.printStackTrace();
            throw new JnomicsThriftException(e.toString());
        }finally{
            closeFileSystem(fs);
        }

        if(null == stats)
            return new ArrayList<JnomicsThriftFileStatus>();

        JnomicsThriftFileStatus[] thriftStatuses = new JnomicsThriftFileStatus[stats.length];
        FileStatus c;
        for(int i=0; i< stats.length; ++i){
            c = stats[i];
            thriftStatuses[i] = new JnomicsThriftFileStatus(c.isDir(),
                    c.getPath().toString(),
                    c.getOwner(),
                    c.getGroup(),
                    c.getPermission().toString(),
                    c.getReplication(),
                    c.getModificationTime(),
                    c.getBlockSize(),
                    c.getLen()
            );
        }
        
        return Arrays.asList(thriftStatuses);
    }
	@Override
	public boolean checkFileStatus(String path, Authentication auth) throws TException, JnomicsThriftException {
        String username;
        if(null == (username = authenticator.authenticate(auth))){
            throw new JnomicsThriftException("Permission Denied");
        }
        
        log.info("Getting file status of "+ path + " for user "+ username);
        
        FileSystem fs = getFileSystem(username);
        boolean ret; 
        try{
            ret = fs.exists(new Path(path));
        }catch(Exception e){
            log.error("Could not open filesystem in checkFileStatus");
            e.printStackTrace();
            throw new JnomicsThriftException(e.toString());
        }finally{
            closeFileSystem(fs);
        }
        return ret;
	}
	
    @Override
    public boolean remove(String path, boolean recursive, Authentication auth) throws JnomicsThriftException, TException {
        String username;
        if(null == (username = authenticator.authenticate(auth))){
            throw new JnomicsThriftException("Permission Denied");
        }

        log.info("Removing path "+ path+ " for user "+ username);
        
        FileSystem fs = getFileSystem(username);
        boolean state = false;
        try{
            state = fs.delete(new Path(path), recursive);
        }catch(Exception e){
            log.error("Problem deleting " + path + " for user: " + username);
            e.printStackTrace();
            throw new JnomicsThriftException(e.toString());
        }finally{
            closeFileSystem(fs);
        }
        return state;
    }

    @Override
    public boolean mkdir(String path, Authentication auth) throws JnomicsThriftException, TException {
        String username;
        if(null == (username = authenticator.authenticate(auth))){
            throw new JnomicsThriftException("Permission Denied");
        }
        log.info("Making directory "+ path + " for user " + username);
        
        FileSystem fs = getFileSystem(username);
        boolean state = false;
        try{
            state = fs.mkdirs(new Path(path));
        }catch(Exception e){
            log.error("Problem making directory "+ path + " for user: " + username);
            e.printStackTrace();
            throw new JnomicsThriftException(e.toString());
        }finally{
            closeFileSystem(fs);
        }
        return state;
    }

    @Override
    public boolean mv(String path, String dest, Authentication auth) throws JnomicsThriftException, TException {
        String username;
        if(null == (username = authenticator.authenticate(auth))){
            throw new JnomicsThriftException("Permission Denied");
        }
        
        log.info("Moving "+ path +" to "+ dest +  " for user "+ username);
        
        FileSystem fs = getFileSystem(username);
        boolean state = false;
        try {
            state = fs.rename(new Path(path),new Path(dest));
        } catch (IOException e) {
            throw new JnomicsThriftException(e.toString());
        }finally{
            closeFileSystem(fs);
        }
        return state;
    }

    @Override
    public List<String> listGenomes(Authentication auth) throws JnomicsThriftException, TException {
        String username;
        if(null == (username = authenticator.authenticate(auth))){
            throw new JnomicsThriftException("Permission Denied");
        }
        
        log.info("Listing genomes for user "+ username);
        
        FileSystem fs = getFileSystem(username);
        List<String> genomeList = new ArrayList<String>();
        try {
            FileStatus[] stats = fs.listStatus(new Path(properties.getProperty("hdfs-index-repo")));
            for(FileStatus stat: stats){
                String name = stat.getPath().getName();
                if(name.contains("_samtools.tar.gz")){
                    genomeList.add(name.substring(0,name.indexOf("_samtools.tar.gz")));
                }
            }
        } catch (IOException e) {
            throw new JnomicsThriftException(e.toString());
        }finally{
            closeFileSystem(fs);
        }

        return genomeList;
    }


}
