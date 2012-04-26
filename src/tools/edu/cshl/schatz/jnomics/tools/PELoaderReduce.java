package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.io.ThreadedStreamConnector;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer;
import edu.cshl.schatz.jnomics.ob.writable.StringArrayWritable;
import edu.cshl.schatz.jnomics.util.FileUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

/**
 * User: james
 */
public class PELoaderReduce extends JnomicsReducer<IntWritable, StringArrayWritable, Text, NullWritable> {

    final Log LOG = LogFactory.getLog(PELoaderReduce.class);
    
    private final Text output = new Text();

    private final JnomicsArgument s3cmd_binary_arg = new JnomicsArgument("s3cmd_binary","s3cmd binary file",true);
    private final JnomicsArgument s3cmd_config_arg = new JnomicsArgument("s3cmd_config","s3cmd configuration file",true);
    private final JnomicsArgument max_sleep_arg = new JnomicsArgument("max_sleep_time","Time to sleep before beginning download",false);
    private String s3cmd_binary;
    private String s3cmd_config;
    private int max_sleeptime;
    
    @Override
    public Class getOutputKeyClass() {
        return Text.class;
    }

    @Override
    public Class getOutputValueClass() {
        return NullWritable.class;
    }

    @Override
    public JnomicsArgument[] getArgs() {
        //return new JnomicsArgument[]{s3cmd_binary_arg,s3cmd_config_arg};
        return new JnomicsArgument[0];
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        /*Configuration conf = context.getConfiguration();
        s3cmd_binary = conf.get(s3cmd_binary_arg.getName());
        s3cmd_config = conf.get(s3cmd_config_arg.getName());
        if(!new File(s3cmd_binary).exists())
            throw new IOException("Cannot find s3cmd_binary file: " + s3cmd_binary );
        if(!new File(s3cmd_config).exists())
            throw new IOException("Cannot find s3cmd_config file" + s3cmd_config);
        max_sleeptime = Integer.parseInt(conf.get(max_sleep_arg.getName(),"20"));*/
    }

    @Override
    protected void reduce(IntWritable key, Iterable<StringArrayWritable> values, final Context context) throws IOException,
            InterruptedException {

        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);

        for (StringArrayWritable maniline : values) {
            //wait a random number of minutes before downloading (so all processes don't hit at once)
            /*
            Random random = new Random();
            int r = random.nextInt(max_sleeptime);
            System.out.println("sleeping for " + r + " minutes");
            for(int i=0;i<r;i++){
                Thread.sleep(60000);
                context.progress();
            }
            System.out.println("done sleeping");
            */
            String[] data = maniline.toStrings();
            System.out.println("data:" +  data);
            Path s3p1 = new Path(data[0]);
            Path s3p2 = new Path(data[1]);
            System.out.println(s3p1);
            System.out.println(s3p2);

            /*Thread connectErr,connectOut;
            for(Path s3Path: new Path[]{s3p1,s3p2}){
                String cmd = String.format("%s -c %s get %s",s3cmd_binary,s3cmd_config,s3Path);
                System.out.println("Running " + cmd);
                Process get = Runtime.getRuntime().exec(cmd);
                connectErr = new Thread(new ThreadedStreamConnector(get.getErrorStream(),System.err));
                connectOut = new Thread(new ThreadedStreamConnector(get.getInputStream(),System.out));
                connectErr.start();connectOut.start();
                get.waitFor();
                connectErr.join();connectOut.join();
                context.progress();
            }

            File s3p1_local = new File(s3p1.getName());
            File s3p2_local = new File(s3p2.getName());
            InputStream in1 = FileUtil.getInputStreamFromExtension(s3p1_local);
            InputStream in2 = FileUtil.getInputStreamFromExtension(s3p2_local);



            System.out.println(s3p1.getName());
            System.out.println(s3p2.getName());
            */
            FileSystem fileSystem = FileSystem.get(s3p1.toUri(),conf);
            InputStream in1 = FileUtil.getInputStreamWrapperFromExtension(fileSystem.open(s3p1),
                    FileUtil.getExtension(s3p1.getName()));
            InputStream in2 = FileUtil.getInputStreamWrapperFromExtension(fileSystem.open(s3p2),
                    FileUtil.getExtension(s3p2.getName()));

            String outName = s3p1.getName() + ".pe";
            Path outPath = new Path(conf.get("mapred.output.dir","") +"/"+ outName);
            try{
                new PairedEndLoader(){
                    @Override
                    protected void progress() {
                        context.progress();
                    }
                    @Override
                    protected Configuration getConf(){
                        return context.getConfiguration();
                    }
                }.load(in1,in2,outPath);
            }catch(Exception e){
                LOG.error(e.toString());
                System.err.println(e.toString());
                continue; // go to next file
            }
        }
    }
}
