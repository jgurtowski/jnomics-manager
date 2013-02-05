package edu.cshl.schatz.jnomics.manager.client.fs;

import edu.cshl.schatz.jnomics.manager.api.JnomicsThriftFileStatus;
import edu.cshl.schatz.jnomics.manager.api.JnomicsThriftHandle;
import edu.cshl.schatz.jnomics.manager.client.ann.Flag;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;

/**
 * User: james
 */
public class Get extends FSBase{

    @Flag(shortForm = "-h", longForm = "--help")
    public boolean help;

    @Override
    public void handle(List<String> remainingArgs, Properties properties) throws Exception {
        super.handle(remainingArgs, properties);

        if(remainingArgs.size() < 1 || help){
            System.out.println("-get <hdfs_file> [local_destination_name]");
            return;
        }else{
            String remoteFile = remainingArgs.get(0);
            List<JnomicsThriftFileStatus> stats  = client.listStatus(remoteFile, auth);
            if(1 != stats.size())
                throw new Exception("remote must be a single file");

            long remoteLen = stats.get(0).getLength();

            JnomicsThriftHandle handle = client.open(remoteFile, auth);
            String local = new File(remoteFile).getName();
            if(remainingArgs.size() == 2)
                local = remainingArgs.get(1);

            File localFile = new File(local);
            if(localFile.exists()){
                client.close(handle,auth);
                throw new Exception("Local File: " + localFile + " already exists");
            }
            OutputStream localOut = new FileOutputStream(localFile);

            ByteBuffer buffer;
            long totalTransfer = 0;
            while((buffer = client.read(handle,auth)).remaining() > 0){
                localOut.write(buffer.array());
                totalTransfer += buffer.remaining();
                System.out.print("\r"+totalTransfer+"/"+remoteLen+" " + ((float)totalTransfer)/remoteLen * 100 + "%");
            }
            System.out.println();
            localOut.close();
            client.close(handle,auth);
        }
    }
}
