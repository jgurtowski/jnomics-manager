package edu.cshl.schatz.jnomics.test.core;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;

/**
 * User: james
 * Test Kmerlookup
 */
public class KmerlookupTest {

    public static void main(String []args) throws IOException, InterruptedException {

        //String cmd = String.format("%s %s",
       //        "/home/james/workspace/kmerlookup/kmerlookup",
        //"/home/james/workspace/kmerlookup/kmer.db");
        //Process p = Runtime.getRuntime().exec(cmd);
        Process p = new ProcessBuilder("/home/james/workspace/kmerlookup/kmerlookup",
                "/home/james/workspace/kmerlookup/kmer.db").start();
        
        System.out.println("Running");
        OutputStream out = p.getOutputStream();
        BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
        System.out.println("writing");
        out.write("TTTTCTTAAAAAAAATTAAAAAA".getBytes());
        out.write("\n".getBytes());
        out.flush();
        System.out.println(in.readLine());
        out.write("TTTTCTTAAAAAAAATTAAAAAA".getBytes());
        out.write("\n".getBytes());
        out.flush();
        System.out.println(in.readLine());
        out.write("TTTTCTTAAAAAAAATTAAAAAA".getBytes());
        out.write("\n".getBytes());
        out.flush();
        System.out.println(in.readLine());
        
        Thread.sleep(60000);
        out.close();
        p.waitFor();
    
        System.out.println("Done");
    }
}
