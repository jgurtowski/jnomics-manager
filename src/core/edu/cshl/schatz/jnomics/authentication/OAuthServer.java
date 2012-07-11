package edu.cshl.schatz.jnomics.authentication;

import net.oauth.*;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.*;
import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

public class OAuthServer {

	public static boolean checkCredentials(String signedurl) throws UnsupportedEncodingException {
		boolean isValid = false;

		// parse the signed url to determine the key, URL, other fields
		
		String username = null;
		
		System.err.println("\n\n==================\n\n");
		System.err.println("checking " + signedurl);
		
        int q = signedurl.indexOf('?');
        
        String baseurl = null;
        String paramstr = null;
        if (q >= 0) {
            baseurl  = signedurl.substring(0, q);
            paramstr = signedurl.substring(q+1);
	    }

	     System.err.println("Baseurl: " + baseurl);
	     System.err.println("Parsing: " + paramstr);
                
	     String[] paramarr = paramstr.split("&");  
	     Set<Entry<String,String>> parammap = new HashSet<Entry<String,String>>();  
	     for (String param : paramarr)  
	     {  
             String pair[] = param.split("=");
             String name = URLDecoder.decode(pair[0],"UTF-8");
	         String value = URLDecoder.decode(pair[1],"UTF-8");
	         
	         System.err.println("adding: \"" + name + "\" = \"" + value + "\"");
	         parammap.add(new AbstractMap.SimpleEntry<String,String>(name, value));
	         
	         if (name.equals("oauth_consumer_key"))
	         {
	        	 username = value;
	         }
	     }
	     
	     if (username == null)
	     {
	    	System.err.println("Can't determine username");
	    	return false;
	     }
	     

		try {
			
			// Lookup the consumer's secret
			String authserver = "http://140.221.92.45/oauthkeys/" + username;
			System.err.println("\n\nAsking authserver for secret: " + authserver);

			URL url = new URL(authserver);
			HttpURLConnection request = (HttpURLConnection) url.openConnection();
			request.connect();

			System.err.println("Request status: " + request.getResponseCode());

			if (request.getResponseCode() == 200) 
			{
				InputStream in = new BufferedInputStream(request.getInputStream());

				byte data[] = new byte[1024];
				int count;

				String jsonResponse = new String();

				while ((count = in.read(data, 0, 1024)) != -1) {
					jsonResponse += new String(data, 0, count);
				}

				//System.err.println("Server Response: " + jsonResponse);

				Object obj = JSONValue.parse(jsonResponse);

				JSONObject jo = (JSONObject) obj;

				//System.err.println("jo:" + jo.toString());

				if (jo.containsKey(username)) 
				{
					JSONObject vals = (JSONObject) jo.get(username);

					String OAUTH_SECRET_FIELD = "oauth_secret";

					if (vals.containsKey(OAUTH_SECRET_FIELD)) {
						String secretkey = (String) vals.get(OAUTH_SECRET_FIELD);
						System.err.println("Found secret from server: \"" + secretkey + "\"");
						
						System.err.println("trying to validate\n");
						
						SimpleOAuthValidator validator = new SimpleOAuthValidator();
						
						OAuthMessage oam = new OAuthMessage("GET", baseurl, parammap);
						
						OAuthServiceProvider osp = new OAuthServiceProvider("", "", "");
						
						OAuthConsumer oac = new OAuthConsumer(null, username, secretkey, osp);

						OAuthAccessor oaa = new OAuthAccessor(oac);
						
						validator.validateMessage(oam, oaa); // throws exception if not valid
						
											
						{
							isValid = true;
						}
					}
				}
			}
		} catch (Exception e) {
			System.err.println("Error trying to authenticate");
			e.printStackTrace();
		}
		
		return isValid;
	}

}
