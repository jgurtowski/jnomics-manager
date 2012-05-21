package edu.cshl.schatz.jnomics.manager.server;

import edu.cshl.schatz.jnomics.manager.api.JnomicsThriftException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;

import java.security.PrivilegedExceptionAction;
import java.util.Properties;

/**
 * User: james
 */
public abstract class JobClientRunner<T> {

    public abstract T jobClientTask() throws Exception;

    private String username;
    private Properties properties;
    private Configuration configuration;
    private JobConf jConf;
    private JobClient jobClient;

    public JobClientRunner(String username, Configuration conf, Properties properties) throws JnomicsThriftException{
        this.username = username;
        this.configuration = conf;
        this.properties = properties;
        final String jobTracker = properties.getProperty("mapreduce-jobtracker-host");
        final int jobTrackerPort = Integer.parseInt(properties.getProperty("mapreduce-jobtracker-port"));
        configuration.set("mapred.job.tracker",jobTracker+":"+jobTrackerPort);
        try {
            jConf = UserGroupInformation.createRemoteUser(username).doAs(new PrivilegedExceptionAction<JobConf>() {
                @Override
                public JobConf run() throws Exception {
                    return new JobConf(configuration);
                }
            });
        }catch(Exception e){
            throw new JnomicsThriftException(e.toString());
        }
        try{
            jobClient = UserGroupInformation.createRemoteUser(username).doAs(new PrivilegedExceptionAction<JobClient>() {
                @Override
                public JobClient run() throws Exception {
                    return new JobClient(jConf);
                }
            });
        }catch(Exception e){
            throw new JnomicsThriftException(e.toString());
        }
    }

    public JobConf getJobConf(){
        return jConf;
    }

    public JobClient getJobClient(){
        return jobClient;
    }

    public T run() throws JnomicsThriftException{
        try{
            return UserGroupInformation.createRemoteUser(username).doAs(new PrivilegedExceptionAction<T>() {
                @Override
                public T run() throws Exception{
                    return jobClientTask();
                }
            });
        }catch(Exception e){
            throw new JnomicsThriftException(e.toString());
        }
    }
}
