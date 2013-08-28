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
public class ShockStage extends FSBase{

    @Flag(shortForm = "-h", longForm = "--help")
    public boolean help;

    @Parameter(shortForm = "-shock_id", longForm = "--shockID", description = "Shock File ID")
    public String shock_id;
    
    @Parameter(shortForm = "-dest", longForm = "--clusterDest", description = "Destination on the Cluster")
    public String dest;
    
    
    @Override
    public void handle(List<String> remainingArgs, Properties properties) throws Exception {
        super.handle(remainingArgs, properties);
         
        if(help || shock_id == null || dest == null){
        	System.out.println("stage_shock -shock_id=<id> -dest=<dest>");
        }else{
        	client.ShockRead(shock_id, dest, auth);
        }
        //String nodeid;
        //File inFile = new File(remainingArgs.get(0));
//        if(!inFile.exists()){
//            throw new Exception("Can't find file: "+ inFile.toString());
//        }
        /*if(remainingArgs.size() < 1 || help){
            System.out.println("-stage_shock <ShockNodeId> [destination]");
            return;
        }else{
        	nodeid = remainingArgs.get(0);
            String dest = "";
            if(1 == remainingArgs.size()){
                dest = ".";
            }else{
                dest = remainingArgs.get(1);
            }
//            if(0 == dest.compareTo("."))
//                dest = inFile.getName();
            if(client.ShockRead(nodeid,dest,auth)) {

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
            System.out.println("Staged"+remainingArgs.get(0)+" to "+remainingArgs.get(1));
            }
            else {
            	System.out.println("Failed to Stage "+remainingArgs.get(0)+" to "+remainingArgs.get(1));
            }
        }*/
    }
}
