package edu.cshl.schatz.jnomics.manager.client.compute;

import edu.cshl.schatz.jnomics.manager.api.JnomicsThriftFileStatus;
import edu.cshl.schatz.jnomics.manager.api.JnomicsThriftJobID;
import edu.cshl.schatz.jnomics.manager.client.Utility;
import edu.cshl.schatz.jnomics.manager.client.ann.Flag;
import edu.cshl.schatz.jnomics.manager.client.ann.FunctionDescription;
import edu.cshl.schatz.jnomics.manager.client.ann.Parameter;
import edu.cshl.schatz.jnomics.manager.common.KBaseIDTranslator;
import edu.cshl.schatz.jnomics.manager.client.fs.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * User: Sri
 */


@FunctionDescription(description = "Converts fastq files to a single .pe file"
)
public class FastqtoPe extends ComputeBase {

    @Flag(shortForm = "-h",longForm = "--help")
    public boolean help;
    
    @Parameter(shortForm = "-file1", longForm = "--file1", description = "fastq file 1")
    public String file1;
    
    @Parameter(shortForm = "-file2", longForm = "--file1", description="fastq file 2")
    public String file2;
    
    @Parameter(shortForm = "-out", longForm= "--output", description = "output (directory)")
    public String out;
    
    @Parameter(shortForm = "-working_dir", longForm = "--working_dir", description = "workingdir (optional)")
    public String working_dir;
    
    @Override
    public void handle(List<String> remainingArgs,Properties properties) throws Exception {

        super.handle(remainingArgs,properties);
        if(help){
            System.out.println(Utility.helpFromParameters(this.getClass()));
            return;
        }else if(null == file1){
            System.out.println("missing -file1 parameter");
        }else if(null == file2){
            System.out.println("missing -file2 parameter");
        }else if(null == out){
            System.out.println("missing -out parameter");
        }else if(fsclient.checkFileStatus(out, auth)){
    		System.out.println("ERROR : Output directory already exists");
        }else{
        	boolean check = false;
            if(!fsclient.checkFileStatus(file1, auth)){
            		System.out.println("ERROR : " + file1 + " does'nt exist");
            		return;
            	}
        	if(!fsclient.checkFileStatus(file1, auth)){
        		System.out.println("ERROR : " + file1 + " does'nt exist");
        		return;
        	}
            JnomicsThriftJobID jobID = client.fastqtoPe(
                    file1,
                    file2,
                    out,
                    Utility.nullToString(working_dir),
                    auth);
            System.out.println("Submitted Job: " + jobID.getJob_id());
            return;
        }

        System.out.println(Utility.helpFromParameters(this.getClass()));
    }
}
