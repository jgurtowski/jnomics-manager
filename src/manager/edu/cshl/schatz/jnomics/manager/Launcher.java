package edu.cshl.schatz.jnomics.manager;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: james
 */

public class Launcher implements Watcher, Runnable{

    ZooKeeper zk;

    public Launcher(String server) throws Exception {
        zk = new ZooKeeper(server, 3000, this);
    }

    @Override
    public void process(WatchedEvent event) {
        synchronized (this){
            //only wake up launcher if a task has been added to the queue or a job has finished/failed
            if(event.getType() == Watcher.Event.EventType.NodeChildrenChanged &&
                    (
                            event.getPath().compareTo(JnomicsZoo.QUEUE_ROOT) == 0 ||
                            event.getPath().compareTo(JnomicsZoo.FINISHED_ROOT) == 0 ||
                            event.getPath().compareTo(JnomicsZoo.FAILED_ROOT) == 0
                    )){
                notifyAll();
            }
        }
    }

    public void run() {
        synchronized (this) {
            try {
                Stat stat = new Stat();
                String data;
                Task task;
                boolean depfilled;
                while (true) {
                    List<String> children = zk.getChildren(JnomicsZoo.QUEUE_ROOT, true);
                    List<String> finishedChildren = zk.getChildren(JnomicsZoo.FINISHED_ROOT, true);
                    List<String> failedChildren = zk.getChildren(JnomicsZoo.FAILED_ROOT, true);

                    Collections.sort(children);
                    Collections.sort(finishedChildren);
                    Collections.sort(failedChildren);
                    
                    for (String s : children) { //iterate through children
                        data = new String(zk.getData(JnomicsZoo.QUEUE_ROOT+"/"+s,false,stat));
                        task = Task.fromJson(data);
                        depfilled = true;
                        //check dependencies have finished
                        for(String depend: task.getDependencies()){
                            if(zk.exists(JnomicsZoo.FAILED_ROOT+"/"+depend, false) != null){
                                //a dependency has failed, this task must fail as well
                                try{ //get lock and move node to failed
                                    zk.delete(JnomicsZoo.QUEUE_ROOT+"/"+s,stat.getVersion());
                                    task.setError("failed dependency "+ depend);
                                    zk.create(JnomicsZoo.FAILED_ROOT+"/"+s,task.toJson().getBytes(),
                                            ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
                                }catch(Exception e){}//didn't get lock 
                                depfilled = false;
                                break;
                            }
                            if( zk.exists(JnomicsZoo.FINISHED_ROOT+"/"+depend, false) == null){
                                depfilled = false;
                                break;
                            }
                        }
                        if(depfilled){ //all dependencies are filled
                            try{ //try to get a lock and run job
                                zk.delete(JnomicsZoo.QUEUE_ROOT+"/"+s,stat.getVersion());
                                zk.create(JnomicsZoo.RUNNING_ROOT+"/"+s,data.getBytes(),
                                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                                int status = task.launch();//run job
                                zk.delete(JnomicsZoo.RUNNING_ROOT+"/"+s,-1);
                                if(status == 0){ //job success move to finished queue
                                    zk.create(JnomicsZoo.FINISHED_ROOT+"/"+s,data.getBytes(),
                                            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                                }else{ //job failed write error to failed queue
                                    zk.create(JnomicsZoo.FAILED_ROOT+"/"+s,task.toJson().getBytes(),
                                            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                                }
                            }catch(Exception e){} //didn't get lock, continue
                        }
                    }//finished iterating through children

                    try {
                        wait(); //wait until zookeeper alerts us that a queue has changed
                    } catch (InterruptedException e) {
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        new Launcher("localhost:2181/jnomics").run();
    }
}
