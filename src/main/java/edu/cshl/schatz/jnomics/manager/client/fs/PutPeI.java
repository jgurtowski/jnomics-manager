package edu.cshl.schatz.jnomics.manager.client.fs;

import edu.cshl.schatz.jnomics.manager.client.ann.Flag;
import edu.cshl.schatz.jnomics.manager.fs.JnomicsThriftFileSystem;
import edu.cshl.schatz.jnomics.tools.PairedEndLoader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

/**
 * User: james
 */
public class PutPeI extends FSBase{

    @Flag(shortForm = "-h", longForm = "--help")
    public boolean help;

    @Override
    public void handle(List<String> remainingArgs, Properties properties) throws Exception {
        super.handle(remainingArgs, properties);

        if(remainingArgs.size() < 2 || help){
            System.out.println("fs -put_pe_i <reads.fq> <output.pe>");
        }else{
            InputStream infile1 = edu.cshl.schatz.jnomics.util.FileUtil.getInputStreamWrapperFromExtension(
                    new FileInputStream(remainingArgs.get(0)),
                    edu.cshl.schatz.jnomics.util.FileUtil.getExtension(remainingArgs.get(0)));
            FileSystem fs = new JnomicsThriftFileSystem(client,auth);
            new PairedEndLoader().load(infile1,new Path(remainingArgs.get(1)+".pe"),fs);
        }
    }
}
