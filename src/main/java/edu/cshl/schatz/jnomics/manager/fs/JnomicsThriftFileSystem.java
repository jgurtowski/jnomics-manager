package edu.cshl.schatz.jnomics.manager.fs;

import edu.cshl.schatz.jnomics.manager.api.Authentication;
import edu.cshl.schatz.jnomics.manager.api.JnomicsData;
import edu.cshl.schatz.jnomics.manager.api.JnomicsThriftFileStatus;
import edu.cshl.schatz.jnomics.manager.api.JnomicsThriftHandle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * User: james
 */
public class JnomicsThriftFileSystem extends FileSystem {

    private JnomicsData.Client client;
    private Authentication auth;

    private Path workingDir;

    private Configuration conf = new Configuration();

    /** Writing to Stream Hacked**/
    private class JnomicsThriftOutputStream extends OutputStream {

        private JnomicsThriftHandle handle;
        int BUFSIZE= 2000000;
        int current = 0;
        byte[] writeBuffer = new byte[BUFSIZE];

        public JnomicsThriftOutputStream(JnomicsThriftHandle handle){
            this.handle = handle;
        }

        @Override
        public void flush() throws IOException {
            writeBuff();
        }

        public void writeBuff() throws IOException{
            if(current < 1)
                return;
            try{
                client.write(handle,ByteBuffer.wrap(writeBuffer,0,current),auth);
            } catch(Exception e){
                throw new IOException(e);
            }
            current = 0;
        }

        @Override
        public void write(int b) throws IOException {
            if(current >= BUFSIZE)
                writeBuff();
            writeBuffer[current++] = (byte)b;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            if(len >= BUFSIZE){
                writeBuff();
                try{
                    client.write(handle,ByteBuffer.wrap(b,off,len),auth);
                }catch(Exception e){
                    throw new IOException(e);
                }
            }else{
                if(current + len > BUFSIZE)
                    writeBuff();
                System.arraycopy(b,off,writeBuffer,current,len);
                current += len;
            }
        }

        @Override
        public void write(byte[] b) throws IOException {
            write(b,0,b.length);
        }

        @Override
        public void close() throws IOException {
            writeBuff();
            try {
                client.close(handle,auth);
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }

    /** Probably does not work **/
    private class JnomicsThriftInputStream extends InputStream {

        public JnomicsThriftHandle handle;

        public JnomicsThriftInputStream(JnomicsThriftHandle handle){
            this.handle = handle;
        }
        
        @Override
        public int read() throws IOException {
            ByteBuffer buffer;
            try {
                buffer = client.read(handle,auth);
            } catch (Exception e){
                throw new IOException(e);
            }
            return buffer.array().length;
        }

        @Override
        public int read(byte[] b) throws IOException {
            ByteBuffer buffer;
            try{
                buffer = client.read(handle,auth);
            } catch (Exception e){
                throw new IOException(e);
            }
            b = buffer.array();
            return b.length;
        }

        @Override
        public void close() throws IOException {
            try{
                client.close(handle,auth);
            }catch(Exception e){
                throw new IOException(e);
            }
        }
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    public JnomicsThriftFileSystem(JnomicsData.Client client, Authentication auth){
        this.client = client;
        this.auth = auth;
    }

    
    @Override
    public URI getUri() {
        return URI.create("jtfs://");
    }

    /**Most Likely does not work properly**/
    @Override
    public FSDataInputStream open(Path path, int i) throws IOException {
        JnomicsThriftHandle handle;
        try {
            handle = client.open(path.toString(),auth);
        } catch (Exception e){
            throw new IOException(e);
        }
        return new FSDataInputStream(new JnomicsThriftInputStream(handle));
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean b, 
                                     int i, short s, long l, Progressable progressable) throws IOException {
        JnomicsThriftHandle h;
        try {
            h = client.create(path.toString(),auth);
        } catch (Exception e){
            throw new IOException(e);
        }
        return new FSDataOutputStream(new JnomicsThriftOutputStream(h));
    }

    @Override
    public FSDataOutputStream append(Path path, int i, Progressable progressable) throws IOException {
        System.err.println("WARNING append not implemented on JnomicsThriftFileSystem");
        throw new IOException("Append not implemented");
    }

    @Override
    public boolean rename(Path path, Path path1) throws IOException {
        System.err.println("WARNING rename not implemented on JnomicsThriftFileSystem");
        throw new IOException("rename not implemented");
    }

    @Override
    public boolean delete(Path path) throws IOException {
        return delete(path,false);
    }

    @Override
    public boolean delete(Path path, boolean b) throws IOException {
        boolean stat = false;
        try {
            stat = client.remove(path.toString(),b,auth);
        } catch (Exception e){
            throw new IOException(e);
        }
        return stat;
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        List<JnomicsThriftFileStatus> statuses;
        try {
            statuses = client.listStatus(path.toString(), auth);
        } catch (Exception e){
            throw new IOException(e);
        }
        
        FileStatus[] statArr = new FileStatus[statuses.size()];
        int i=0;
        for(JnomicsThriftFileStatus s: statuses){
            statArr[i++] = new FileStatus(s.length,s.isDir,s.replication,s.block_size,s.mod_time,new Path(s.path));
        }
        return statArr;
    }

    @Override
    public void setWorkingDirectory(Path path) {
        workingDir = path;
    }

    @Override
    public Path getWorkingDirectory() {
        return workingDir;
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
        boolean b;
        try {
            b = client.mkdir(path.toString(),auth);
        } catch (Exception e) {
            throw new IOException(e);
        }
        return b;
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        FileStatus[] stats = listStatus(path);
        if(stats.length != 1)
            return null;
        return stats[0];
    }
}
