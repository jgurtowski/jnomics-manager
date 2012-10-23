package edu.cshl.schatz.jnomics.manager.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 * User: james
 */
public class JnomicsHandleGarbageCollector implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(JnomicsHandleGarbageCollector.class);
    private static final int SLEEP_INTERVAL = 60 * 1000 * 4;

    private Map<UUID,JnomicsFsHandle> handles;

    public JnomicsHandleGarbageCollector(JnomicsDataHandler handler) {
        handles = handler.getHandleMap();
    }

    @Override
    public void run() {
        while(true){
            try {
                Thread.sleep(SLEEP_INTERVAL);
            } catch (InterruptedException e) {
                break;
            }
            JnomicsFsHandle handle;
            UUID uuid;
            int count = 0;
            for(Map.Entry<UUID,JnomicsFsHandle> entry: handles.entrySet()){
                uuid = entry.getKey();
                handle = entry.getValue();

                if(System.currentTimeMillis() - handle.getLastUsed() > SLEEP_INTERVAL){
                    if(handle.getOutStream() != null){
                        try {
                            handle.getOutStream().close();
                        } catch (IOException e) {
                        }
                    }else if(handle.getInStream() != null){
                        try {
                            handle.getInStream().close();
                        } catch (IOException e) {
                        }
                    }
                    try {
                        handle.getFileSystem().close();
                    } catch (IOException e) {
                    } finally{
                        handles.remove(uuid);
                        count++;
                    }
                }
            }
            logger.info("FS Handle Garbage Collector removed "+count+" unused handles");
        }
    }
}
