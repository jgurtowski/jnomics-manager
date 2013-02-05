package edu.cshl.schatz.jnomics.manager.client.old;

import edu.cshl.schatz.jnomics.io.FastqParser;
import edu.cshl.schatz.jnomics.manager.fs.JnomicsThriftFileSystem;
import edu.cshl.schatz.jnomics.manager.api.*;
import edu.cshl.schatz.jnomics.ob.ReadCollectionWritable;
import edu.cshl.schatz.jnomics.ob.ReadWritable;
import edu.cshl.schatz.jnomics.tools.PairedEndLoader;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;

/**
  * User: james
 */
public class FSHandler extends HandlerBase {

    private static final Options opts = new Options();

    static{
        opts.addOption("Ls",false,"list contents of directory");
        opts.addOption("put",false,"upload raw file");
        opts.addOption("get",false,"download file");
        opts.addOption("rm", false, "remove file");
        opts.addOption("rmr", false, "remove recursive (directory)");
        opts.addOption("put_pe",false,"upload paired end sequencing file");
        opts.addOption("put_pe_i",false,"upload paired end sequencing file (reads interleaved, single file)");
        opts.addOption("put_se",false,"upload single end sequencing file");
        opts.addOption("mkdir", false, "make a directory");
        opts.addOption("mv",false,"Move file or directory");
    }

    public void handle(List<String> args, Properties properties) throws Exception {

        HelpFormatter f = new HelpFormatter();
        
        if(0 == args.size()){
            f.printHelp("Help",opts);
            return;
        }
        final JnomicsData.Client client = JnomicsThriftClient.getFsClient(properties);

        String username = properties.getProperty("username");
        String password = properties.getProperty("password");
        String token = properties.getProperty("token");
        final Authentication auth = new Authentication(username,password,token);
        BasicParser parser = new BasicParser();

        CommandLine cl = parser.parse(opts,args.toArray(new String[0]));

        @SuppressWarnings("unchecked")
        List<String> arglist = cl.getArgList();

        if(cl.hasOption("Ls")){ /****************Ls ****************/
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
        }else if(cl.hasOption("put_pe")){ /*************************** put-pe *******************/
            if(arglist.size() < 3){
                f.printHelp("fs -put_pe <reads.1.fq> <reads.2.fq> <output.pe>",opts);
            }else{
                InputStream infile1 = edu.cshl.schatz.jnomics.util.FileUtil.getInputStreamWrapperFromExtension(
                        new FileInputStream(arglist.get(0)),
                        edu.cshl.schatz.jnomics.util.FileUtil.getExtension(arglist.get(0)));
                InputStream infile2 = edu.cshl.schatz.jnomics.util.FileUtil.getInputStreamWrapperFromExtension(
                        new FileInputStream(arglist.get(1)),
                        edu.cshl.schatz.jnomics.util.FileUtil.getExtension(arglist.get(1)));

                FileSystem fs = new JnomicsThriftFileSystem(client,auth);
                new PairedEndLoader().load(infile1,infile2,new Path(arglist.get(2)+".pe"),fs);
            }
        }else if(cl.hasOption("put_pe_i")){
            if(arglist.size() < 2){
                f.printHelp("fs -put_pe_i <reads.fq> <output.pe>",opts);
            }else{
                InputStream infile1 = edu.cshl.schatz.jnomics.util.FileUtil.getInputStreamWrapperFromExtension(
                        new FileInputStream(arglist.get(0)),
                        edu.cshl.schatz.jnomics.util.FileUtil.getExtension(arglist.get(0)));
                FileSystem fs = new JnomicsThriftFileSystem(client,auth);
                new PairedEndLoader().load(infile1,new Path(arglist.get(1)+".pe"),fs);
            }
        }else if(cl.hasOption("put_se")){ /************************** put-se *********************/
            if(arglist.size() < 2){
                f.printHelp("fs -put_se <reads.fq> <output.se>",opts);
            }else{

                InputStream infile1 = edu.cshl.schatz.jnomics.util.FileUtil.getInputStreamWrapperFromExtension(
                        new FileInputStream(arglist.get(0)),
                        edu.cshl.schatz.jnomics.util.FileUtil.getExtension(arglist.get(0)));
                FastqParser parser1 = new FastqParser(infile1);
                String outFile = arglist.get(1).concat(".se");

                NullWritable sfValue = NullWritable.get();
                ReadCollectionWritable sfKey  = new ReadCollectionWritable();

                ReadWritable r1= new ReadWritable();
                sfKey.addRead(r1);
                Text keyName = new Text();
                sfKey.setName(keyName);

                JnomicsThriftFileSystem fs = new JnomicsThriftFileSystem(client,auth);

                SequenceFile.Writer writer = SequenceFile.createWriter(fs,new Configuration(),new Path(outFile),
                        sfKey.getClass(),sfValue.getClass());


                long count = 0;
                for(FastqParser.FastqRecord record: parser1){
                    r1.setAll(record.getName(),record.getSequence(),record.getDescription(),record.getQuality());
                    keyName.set(record.getName());
                    writer.append(sfKey,sfValue);
                    if(0 == ++count % 100000){
                        System.out.println(count);
                    }
                }

                parser1.close();
                infile1.close();
                writer.close();

            }
        
        }else if(cl.hasOption("mkdir")){ /******************************mkdir **********************/
            if(arglist.size() < 1){
                f.printHelp("fs -mkdir <directory>", opts);
            }
            if(client.mkdir(arglist.get(0), auth)){
                System.out.println("Mkdir: "+ arglist.get(0));
            }else{
                System.out.println("Failed to mkdir: "+ arglist.get(0));
            }
        }else if(cl.hasOption("mv")){
            if(arglist.size() < 2){
                f.printHelp("fs -mv <dir/file> <dest>",opts);
            }
            if(client.mv(arglist.get(0),arglist.get(1),auth)){
                System.out.println("Moved "+arglist.get(0)+" to "+arglist.get(1));
            }else{
                System.out.println("Failed to move "+arglist.get(0)+" to "+arglist.get(1));
            }
        }else{
            f.printHelp("Unknown Option",opts);
        }

    }
    @Override
    protected JnomicsArgument[] getArguments() {
        return new JnomicsArgument[0];
    }

}
