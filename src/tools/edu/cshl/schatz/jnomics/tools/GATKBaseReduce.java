package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * User: james
 * Base class reducer for many GATK Operations
 */

public abstract class GATKBaseReduce<KEYIN,KEYOUT,VALIN,VALOUT> extends JnomicsReducer<KEYIN,KEYOUT,VALIN,VALOUT> {

    private final JnomicsArgument gatk_jar_arg = new JnomicsArgument("gatk_jar","GATK jar file",true);
    private final JnomicsArgument samtools_bin_arg = new JnomicsArgument("samtools_binary","Samtools binary file",true);
    private final JnomicsArgument reference_fa_arg = new JnomicsArgument("reference_fa", "Reference genome", true);

    protected Map<String,File> binaries = new HashMap<String, File>();

    protected File samtools_binary, reference_fa, gatk_binary;

    @Override
    public Map<String,String> getConfModifiers(){
        return new HashMap<String, String>(){
            {
                put("mapred.reduce.tasks.speculative.execution","false");
            }
        };
    }

    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        File f = null;
        for(JnomicsArgument binary_arg : getArgs()){
            f =  new File(conf.get(binary_arg.getName(),""));
            if(!f.exists())
                throw new IOException("Missing : " + binary_arg.getName());
            binaries.put(binary_arg.getName(),f);
        }

        samtools_binary = binaries.get(samtools_bin_arg.getName());
        reference_fa = binaries.get(reference_fa_arg.getName());
        gatk_binary = binaries.get(gatk_jar_arg.getName());
    }

    @Override
    public JnomicsArgument[] getArgs() {
        return new JnomicsArgument[]{gatk_jar_arg, samtools_bin_arg,reference_fa_arg};
    }
}
