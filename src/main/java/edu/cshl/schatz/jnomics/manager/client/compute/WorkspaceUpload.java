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

@FunctionDescription(description = "Loads RNASeq Expression Objects to Workspace"
)
public class WorkspaceUpload extends ComputeBase {

    @Flag(shortForm = "-h",longForm = "--help")
    public boolean help;
    
    @Parameter(shortForm = "-kb_id", longForm = "--kb_id", description="Kbase id")
    public String kb_id;
    
    @Parameter(shortForm = "-in", longForm = "--input", description = "full path of the input file")
    public String in;
    
    @Parameter(shortForm = "-genome_id", longForm= "--genome_id", description = "genome id")
    public String genome_id;
    
    @Parameter(shortForm = "-onto_term_id", longForm = "--onto_term_id", description = "Ontology term id (optional)")
    public String onto_term_id;
    
    @Parameter(shortForm = "-onto_term_def", longForm = "--onto_term_def", description = "Ontology term definition (optional)")
    public String onto_term_def;
    
    @Parameter(shortForm = "-onto_term_name", longForm = "--onto_term_name", description = "Ontology term name (optional)")
    public String onto_term_name;
    
    @Parameter(shortForm = "-seq_type", longForm = "--seq_type", description = "Sequence Type (Paired-End or Single-End)(optional)")
    public String seq_type;
    
    @Parameter(shortForm = "-ref", longForm = "--reference", description = "reference genome (optional)")
    public String ref;
    
    @Parameter(shortForm = "-working_dir", longForm = "--working_dir", description = "workingdir (optional)")
    public String working_dir;
    
    @Override
    public void handle(List<String> remainingArgs,Properties properties) throws Exception {

        super.handle(remainingArgs,properties);
        if(help){
            System.out.println(Utility.helpFromParameters(this.getClass()));
            return;
        }else if(null == kb_id){
            System.out.println("missing -kb_id parameter");
        }else if(null == in){
            System.out.println("missing -in parameter");
        }else if(null == genome_id){
            System.out.println("missing -genome_id parameter");
    	}else{
            if(!fsclient.checkFileStatus(in, auth)){
            		System.out.println("ERROR : " + in + " does'nt exist");
            		return;
            }
            String clean_org = KBaseIDTranslator.translate(ref);
            JnomicsThriftJobID jobID = client.workspaceUpload(
            		in, 
            		kb_id, 
            		genome_id,
            		Utility.nullToString(onto_term_id),
            		Utility.nullToString(onto_term_def), 
            		Utility.nullToString(onto_term_name), 
            		Utility.nullToString(seq_type),  
            		Utility.nullToString(ref), 
            		auth);

            System.out.println("Submitted Job: " + jobID.getJob_id());
            return;
        }

        System.out.println(Utility.helpFromParameters(this.getClass()));
    }
}
