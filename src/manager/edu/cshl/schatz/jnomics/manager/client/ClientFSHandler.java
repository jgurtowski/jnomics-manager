package edu.cshl.schatz.jnomics.manager.client;

import edu.cshl.schatz.jnomics.io.FastqParser;
import edu.cshl.schatz.jnomics.manager.api.Authentication;
import edu.cshl.schatz.jnomics.manager.api.JnomicsData;
import edu.cshl.schatz.jnomics.manager.api.JnomicsThriftFileStatus;
import edu.cshl.schatz.jnomics.manager.api.JnomicsThriftHandle;
import edu.cshl.schatz.jnomics.tools.PairedEndLoader;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;

/**
  * User: james
 */
public class ClientFSHandler extends ClientHandler{

    private static final Options opts = new Options();
    private Properties properties;

    static{
        opts.addOption("ls",false,"list contents of directory");
        opts.addOption("put",false,"upload file");
        opts.addOption("get",false,"download file");
        opts.addOption("rm", false, "remove file");
        opts.addOption("rmr", false, "remove recursive (directory)");
        opts.addOption("putpe",false,"upload paired end sequencing file");
    }

    public ClientFSHandler(Properties prop){
        properties = prop;
    }

    public void handle(List<String> args) throws Exception {

        HelpFormatter f = new HelpFormatter();
        
        if(0 == args.size()){
            f.printHelp("Help",opts);
            return;
        }
        String thriftDataHost = properties.getProperty("data-server-host");
        int thriftDataPort = Integer.parseInt(properties.getProperty("data-server-port"));
        TTransport transport = new TSocket(thriftDataHost, thriftDataPort);
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        JnomicsData.Client client = new JnomicsData.Client(protocol);

        String username = properties.getProperty("username");
        String password = properties.getProperty("password");
        Authentication auth = new Authentication(username,password);

        BasicParser parser = new BasicParser();
        CommandLine cl = parser.parse(opts,args.toArray(new String[0]));

        @SuppressWarnings("unchecked")
        List<String> arglist = cl.getArgList();

        if(cl.hasOption("ls")){ /****************ls ****************/
            String dest = ".";
            if(1 == arglist.size()){
                dest = arglist.get(0);
            }
            List<JnomicsThriftFileStatus> stats= client.listStatus(dest, auth);
            System.out.println("Found "+ stats.size() + " items");
            for(JnomicsThriftFileStatus status: stats){
                System.out.printf("%s\t%2d\t%s\t%s\t%14d\t%s\n",
                        status.getPermission(),
                        status.getReplication(),
                        status.getOwner(),
                        status.getGroup(),
                        status.getLength(),
                        status.getPath());
            }
            
        }else if(cl.hasOption("put")){ /********************put**************/
            if(arglist.size() < 1 || arglist.size() > 3 ){
                f.printHelp("fs -put <localFile> [remoteFile]",opts);
            }else{
                File inFile = new File(arglist.get(0));
                if(!inFile.exists()){
                    throw new Exception("Can't find file: "+ inFile.toString());
                }
                String dest = "";
                if(1 == arglist.size()){
                    dest = inFile.getName();
                }else{
                    dest = arglist.get(1);
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
        }else if(cl.hasOption("get")){ /**************get***************/
            if(arglist.size() < 1 || arglist.size() > 3){
                f.printHelp("fs -get <remoteFile> [localfile]",opts);
            }else{
                String remoteFile = arglist.get(0);
                List<JnomicsThriftFileStatus> stats  = client.listStatus(remoteFile, auth);
                if(1 != stats.size())
                    throw new Exception("remote must be a single file");

                long remoteLen = stats.get(0).getLength();

                JnomicsThriftHandle handle = client.open(remoteFile, auth);
                String local = new File(remoteFile).getName();
                if(arglist.size() == 2)
                    local = arglist.get(1);

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
        }else if(cl.hasOption("rm") || cl.hasOption("rmr")){  /*********rm / rmr ****************/
            if(arglist.size() < 1 ){
                f.printHelp("fs -rm <remotefile1> <remotefile2> ...",opts);
            }else{
                boolean recurse = true;
                if(cl.hasOption("rm"))
                    recurse = false;
                boolean status;
                for(String file : arglist){
                    status = client.remove(file,recurse,auth);
                    if(status)
                        System.out.println("Deleted: " + file);
                    else
                        System.out.println("Failed Deleting: " + file);
                }
            }
        }else if(cl.hasOption("put-pe")){ /*************************** put-pe *******************/
            if(arglist.size() < 3){
                f.printHelp("fs -put-pe <read.1.fq> <read.2.fq> <output.pe>",opts);
            }else{
                InputStream infile1 = new FileInputStream(arglist.get(0));
                InputStream infile2 = new FileInputStream(arglist.get(1));
                FastqParser parser1 = new FastqParser(infile1);
                FastqParser parser2 = new FastqParser(infile2);
                String outFile = arglist.get(2).concat(".pe");

                JnomicsThriftHandle handle = client.create(outFile, auth);

                client.close(handle,auth);
            }
        }else{
            f.printHelp("Unknown Option",opts);
        }

        transport.close();

    }

    @Override
    protected JnomicsArgument[] getArguments() {
        return new JnomicsArgument[0];
    }

}
