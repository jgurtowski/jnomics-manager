package edu.cshl.schatz.jnomics.authentication;


import oauth.signpost.OAuthConsumer;
import oauth.signpost.basic.DefaultOAuthConsumer;


public class OAauthClient {
	
	public static void main(String[] args) {
		
		try {
			
			// Create request
			String consumer_key = "key3";
			String consumer_secret = "secret3";
			String urlstr = "http://schatzlab.cshl.edu/data/test";
			
			System.err.println("key: " + consumer_key);
			System.err.println("secret: " + consumer_secret);
			System.err.println("Base URL: " + urlstr);
			
			
			// Sign the request
			OAuthConsumer consumer = new DefaultOAuthConsumer(consumer_key, consumer_secret);
  			String signedurl = consumer.sign(urlstr);
  			
  			System.err.println("Signed URL: " + signedurl);
  			
  			
  			// Check that it is valid
  			boolean valid = OAuthServer.checkCredentials(signedurl);
  			System.err.println("Request was" + valid);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
		}
	}
}
