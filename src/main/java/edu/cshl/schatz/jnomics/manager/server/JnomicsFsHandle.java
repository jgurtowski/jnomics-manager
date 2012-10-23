package edu.cshl.schatz.jnomics.manager.server;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;

/**
 * User: james
 */

public class JnomicsFsHandle{

    private FileSystem fileSystem = null;
    private FSDataOutputStream outStream = null;
    private FSDataInputStream inStream = null;
    private long lastUsed;

    private JnomicsFsHandle(FileSystem fs){
        fileSystem = fs;
        lastUsed = System.currentTimeMillis();
    }

    public JnomicsFsHandle(FileSystem fs, FSDataOutputStream stream){
        this(fs);
        outStream = stream;
    }

    public JnomicsFsHandle(FileSystem fs, FSDataInputStream stream){
        this(fs);
        inStream = stream;
    }
    
    public void updateLastUsed(){
        lastUsed = System.currentTimeMillis();
    }
    
    public FileSystem getFileSystem() {
        return fileSystem;
    }

    public void setFileSystem(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    public FSDataOutputStream getOutStream() {
        return outStream;
    }

    public void setOutStream(FSDataOutputStream outStream) {
        this.outStream = outStream;
    }

    public FSDataInputStream getInStream() {
        return inStream;
    }

    public void setInStream(FSDataInputStream inStream) {
        this.inStream = inStream;
    }

    public long getLastUsed() {
        return lastUsed;
    }

    public void setLastUsed(long lastUsed) {
        this.lastUsed = lastUsed;
    }
}