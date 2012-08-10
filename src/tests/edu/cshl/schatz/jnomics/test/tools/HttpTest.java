package edu.cshl.schatz.jnomics.test.tools;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
  * User: james
 */
public class HttpTest {


    public static void main(String []args) throws IOException, HttpException {
        byte[] buffer = new byte[10240];
        HttpClient client = new HttpClient();
        client.startSession(new URL("http://www.google.com"));
        HttpMethod method = new GetMethod("http://www.google.com");
        client.executeMethod(method);
        InputStream iStream = method.getResponseBodyAsStream();
        int count=0;
        while((count=iStream.read(buffer)) > 0){
            System.out.write(buffer,0,count);
        }
        client.endSession();
    }
}
