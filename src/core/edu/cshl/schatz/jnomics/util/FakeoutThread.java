package edu.cshl.schatz.jnomics.util;

/**
 * User: james
 */

public class FakeoutThread extends Thread{

    public volatile boolean keepRunning = true;

    private long timeout;

    public FakeoutThread(long timeout){
        this.timeout = timeout;
    }

    @Override
    public void run() {
        while(keepRunning){
            try {
                Thread.sleep(timeout);
            } catch (InterruptedException e) {
                break;
            }
            performOperation();
        }
    }

    /**
     * Perform operation in every loop when thread wakes up
     * Designed to be overridden
     */
    public void performOperation(){

    }
}