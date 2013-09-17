package edu.cshl.schatz.jnomics.manager.client;

import edu.cshl.schatz.jnomics.manager.client.ann.Flag;
import edu.cshl.schatz.jnomics.manager.client.ann.KbaseScript;
import edu.cshl.schatz.jnomics.util.ProcessUtil;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;

/**
 * User: james
 */
public class CreateKbaseScripts {

    private static final ArrayList<Class <? extends ClientFunctionHandler>> exportedClasses =
            new ArrayList<Class<? extends ClientFunctionHandler>>(){
        {
            add(FS.class);
            add(Compute.class);
        }
    };

    
    private static final String SCRIPT_TEMPLATE = "#!/bin/bash \n" +
            "SCRIPT_PATH=`dirname \"$0\"`\n" +
            "${SCRIPT_PATH}/jkbase %s %s $*\n";

    
    public static void main(String []args) throws Exception{

        if(2 != args.length ){
            System.out.println("CreateKbaseScripts prefix output_dir");
            return;
        }

        String leadPrefix = args[0];
        String outputDir = args[1];
        
        for(Class<? extends ClientFunctionHandler> c : exportedClasses){

            KbaseScript kann = c.getAnnotation(KbaseScript.class);
            for(String field : kann.exportFields()){
                String[] scriptName = new String[]{leadPrefix,kann.prefix(),field.replace('_', '-')};
                Field f = c.getField(field);
                Flag flag = f.getAnnotation(Flag.class);
                File outFile = new File(outputDir, StringUtils.join(scriptName,"-"));
                FileOutputStream outStream = new FileOutputStream(outFile);
                String scriptContent = String.format(SCRIPT_TEMPLATE, kann.prefix(), flag.shortForm());
                outStream.write(scriptContent.getBytes());
                outStream.close();
                ProcessUtil.execAndReconnect(String.format("chmod +x %s",outFile));
            }
        }
    }
}
