package edu.cshl.schatz.jnomics.manager;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;

public class TestZoo implements Watcher, Runnable{

    private static final String QUEUE_ROOT = "/queue";
    private static final String RUNNING_ROOT = "/running";
    private static final String FINISHED_ROOT = "/finished";
    
    private static final List<ACL> OPEN_ACL = ZooDefs.Ids.OPEN_ACL_UNSAFE;
    private ZooKeeper zk;
    private Stat stat = new Stat();
    public TestZoo() throws Exception{
        zk = new ZooKeeper("localhost:2181/jnomics",3000, this);
    }

    public void process(WatchedEvent event){
        synchronized(this){
            System.out.println(event);
            if(event.getType() == Watcher.Event.EventType.NodeChildrenChanged &&
               (event.getPath().compareTo(QUEUE_ROOT) == 0 || 
		event.getPath().compareTo(FINISHED_ROOT) == 0)){
                notifyAll();
            }
        }
    }

    public static void main(String []args) throws Exception{
        Thread zoo = new Thread(new TestZoo());
        zoo.start();
        zoo.join();
    }

    public void run(){
        try{
            synchronized(this){
		String data;
		String []arr;
                repeater: while(true){
                    System.out.println("hi");
                    List<String> children = zk.getChildren(QUEUE_ROOT,true);
                    List<String> finished_children = zk.getChildren(FINISHED_ROOT,true);
		    Collections.sort(finished_children);
		    Collections.sort(children);
                    for(String s : children){
			try{
			    data = new String(zk.getData(QUEUE_ROOT+"/"+s,false, stat));
			    //check data, move to running queue
			    arr = data.split(","); 
			    if(arr[1].compareTo("NULL") != 0){
				//if the task dependency is not in the finished queue, 
				//then continue to next task
				if( Collections.binarySearch(finished_children,arr[1]) < 0 )
				    continue;
			    }
			    try{//try to lock this node
				zk.delete(QUEUE_ROOT+"/"+s, stat.getVersion());				
				//we have lock on this node, move it to running queue and start process
				zk.create(RUNNING_ROOT+"/"+s,data.getBytes(),OPEN_ACL,CreateMode.PERSISTENT);
				System.out.println("STARTING MY Process for "+ s + "--"+arr[0]);
				Thread.currentThread().sleep(40000);
				System.out.println("DONE, moving to finished");
				zk.delete(RUNNING_ROOT+"/"+s,-1);
				zk.create(FINISHED_ROOT+"/"+s,data.getBytes(), OPEN_ACL, CreateMode.PERSISTENT);
				continue repeater; //recheck starting queue for dependencies
			    }catch (Exception e){
				System.out.println(e);
			    }
			}catch(Exception e){}
                    }
                    wait();
                }
            }
        }catch(Exception e ){
            System.out.println(e);
        }
    }

}


/*String lock_node = zk.create(QUEUE_ROOT+s+"write-","write".getBytes(), OPEN_ACL,
  CreateMode.EPHEMERAL_SEQUENTIAL);
  System.out.prinln(lock_node);
  List<String> lnChildren = zk.getChildren(QUEUE_ROOT+s,false);
  Collections.sort(lnChildren);
  if(lock_node == lnChildren.get(0)){
  String data = zk.getData(QUEUE_ROOT+s,false,new Stat());
  System.out.println(data);
  }*/