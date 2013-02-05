package edu.cshl.schatz.jnomics.manager.client.fs;

import edu.cshl.schatz.jnomics.manager.api.JnomicsThriftHandle;
import edu.cshl.schatz.jnomics.manager.client.ann.Flag;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;

/**
 * User: james
 */
public class Put extends FSBase{

    @Flag(shortForm = "-h", longForm = "--help")
    public boolean help;

    @Override
    public void handle(List<String> remainingArgs, Properties properties) throws Exception {
        super.handle(remainingArgs, properties);

        if(remainingArgs.size() < 1 || help){
            System.out.println("-put <local_file> [destination]");
            return;
        }else{
            File inFile = new File(remainingArgs.get(0));
            if(!inFile.exists()){
                throw new Exception("Can't find file: "+ inFile.toString());
            }
            String dest = "";
            if(1 == remainingArgs.size()){
                dest = inFile.getName();
            }else{
                dest = remainingArgs.get(1);
            }
            if(0 == dest.compareTo("."))
                dest = inFile.getName();

            JnomicsThriftHandle handle = client.create(dest, auth);
            InputStream localStream = new FileInputStream(inFile);

            byte[] buffer = new byte[1000000];
            int amt;
            long total=0;
            long fsize = inFile.length();
            while(-1 != (amt = localStream.read(buffer))){
                total += amt;
                System.out.print("\r"+total+"/"+fsize + " " + ((float)total)/fsize * 100+"%");
                client.write(handle, ByteBuffer.wrap(buffer, 0, amt), auth);
            }
            System.out.println();
            localStream.close();
            client.close(handle,auth);
        }
    }
}
