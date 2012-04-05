package edu.cshl.schatz.jnomics.manager;

import org.apache.zookeeper.*;

import org.apache.hadoop.conf.Configuration;

import java.net.InetSocketAddress;

/**
 * Created by IntelliJ IDEA.
 * User: james
 */

public class HadoopJobMonitor implements Watcher, Runnable{

    ZooKeeper zk;
    //Cluster hadoopCluster;

    public HadoopJobMonitor(String server, String jobtracker) throws Exception {
        zk = new ZooKeeper(server, 3000, this);
        String[] arr = jobtracker.split(":");
        if(arr.length != 2){
            throw new Exception("Malformed jobtracker string must be in form 'server:port'");
        }
        //hadoopCluster = new Cluster(new InetSocketAddress(arr[0], Integer.parseInt(arr[1])),
        //new Configuration());
    }

    @Override
    public void process(WatchedEvent event) {

    }

    public void run() {
        synchronized (this) {

        }
    }

    public static void main(String[] args) throws Exception {
        new HadoopJobMonitor("localhost:2181/jnomics","localhost:9001").run();
    }
}
