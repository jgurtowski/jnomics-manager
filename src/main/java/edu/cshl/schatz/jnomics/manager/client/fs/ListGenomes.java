package edu.cshl.schatz.jnomics.manager.client.fs;

import edu.cshl.schatz.jnomics.manager.client.ann.Flag;

import java.util.List;
import java.util.Properties;

/**
 * User: james
  */
public class ListGenomes extends FSBase {

    @Flag(shortForm = "-h", longForm = "--help")
    public boolean help;

    @Flag(shortForm = "-a", longForm = "--all")
    public boolean all;

    @Flag(shortForm = "-kb", longForm = "--kbase")
    public boolean kbase;
    
    
    @Override
    public void handle(List<String> remainingArgs, Properties properties) throws Exception {
        super.handle(remainingArgs, properties);

        if(help){
            System.out.println("Parameters:");
            System.out.println("-a\tList all Genomes");
            System.out.println("-kb\tList KBase Genomes");
            return;
        }

        List<String> genomes = client.listGenomes(auth);

        System.out.println("Available Genomes:");
        if(0 == genomes.size()){
            System.out.println("None available");
        }else{
            for(String g: genomes){
                if(!all && !kbase && g.startsWith("kb_"))
                    continue;
                if(kbase && !g.startsWith("kb_"))
                    continue;
                if(g.startsWith("kb_")){
                    System.out.println(g.replace("kb_","kb|"));
                }else{
                    System.out.println(g);
                }
            }
        }
    }
}
