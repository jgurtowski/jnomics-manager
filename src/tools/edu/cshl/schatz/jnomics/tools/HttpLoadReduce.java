package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer;
import edu.cshl.schatz.jnomics.ob.writable.StringArrayWritable;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * User: james
 */
public class HttpLoadReduce extends JnomicsReducer<IntWritable, StringArrayWritable, Text, NullWritable> {

    private byte []buffer = new byte[1024];
    private FileSystem fs;
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
        return new JnomicsArgument[0];
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        fs = FileSystem.get(context.getConfiguration());
    }

    @Override
    protected void reduce(IntWritable key, Iterable<StringArrayWritable> values, Context context) throws IOException, InterruptedException {

        HttpClient client = new HttpClient();
        HttpMethod method = new GetMethod("http://www.google.com");
        InputStream iStream = method.getResponseBodyAsStream();
        OutputStream outputStream = null;
        try {
            outputStream = fs.create(new Path("googleout"),false);
            client.executeMethod(method);
            int read;
            while((read = iStream.read(buffer)) > 0){
                outputStream.write(buffer,0,read);
            }
        }catch (HttpException e) {
            context.write(new Text("Failed to fetch"),NullWritable.get());
        }finally{
            iStream.close();
            outputStream.close();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        fs.close();
    }
}
