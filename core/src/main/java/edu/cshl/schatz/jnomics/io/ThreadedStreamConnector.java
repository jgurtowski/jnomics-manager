package edu.cshl.schatz.jnomics.io;


import java.io.InputStream;
import java.io.OutputStream;

/**
 *James
 * Reads from inputstream writes to outputstream
 */
public class ThreadedStreamConnector implements Runnable {

    InputStream in;
    OutputStream out;
    
    public ThreadedStreamConnector(InputStream in, OutputStream out){
        this.in = in;
        this.out = out;
    }

    @Override
    public void run() {
        byte[] data = new byte[1024];
        int len;
        try{
            while((len = in.read(data)) != -1){
                out.write(data,0,len);
                out.flush();
            }
            out.flush();
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
