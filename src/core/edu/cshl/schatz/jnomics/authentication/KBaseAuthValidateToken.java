/** 
 * Server-side authentication of KBase token.
 * This class can be used in a CLI command line way, for example:
 * 	 >java KBaseAuthValidateToken "***"
 * Currently, only one parameter is supported.
 * 
 * @author Shuchu Han
 * @version 1.1
 */

package edu.cshl.schatz.jnomics.authentication;

import java.net.*;
import java.io.*;
import java.util.*;
import java.security.interfaces.RSAPublicKey; //RSA public key
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.Signature; 
import java.security.SignatureException;

import javax.net.ssl.HttpsURLConnection;
import com.mongodb.DBObject;
//import org.json.simple.*; //http://code.google.com/p/json-simple/
import com.mongodb.util.JSON;

/** KBaseAuthValidToken.java
 * 
 * This class is used to verify a Token. A Token is a String and is sent
 * to here by the client. The token contains the URL address of the Authentication
 * Server, and the personalized Key information of the Client.
 * 
 * Once the class received the Token from Client, it does the following jobs:
 * 1. Parses the Token string into a JSON object. 
 * 2. Send a HTTPS request to the Authentication Server address (this address is 
 *    specialized for each client). The response is a Public Key of the client.
 * 3. Once we get the Public key, we are now going to verify the identity of client.
 * 	  The client Token is encrypted with client's private RSA key and by using SHA1 
 *    algorithm.
 *    This class follows the standard Signature way to do the verification. Please reference: 
 *    http://docs.oracle.com/javase/6/docs/technotes/guides/security/crypto/CryptoSpec.html
 *    http://docs.oracle.com/javase/6/docs/technotes/guides/security/crypto/CryptoSpec.html#KeyFactoryEx
 * 
 */
public class KBaseAuthValidateToken {
	
	/**
	 * 
	 * @param params The input parameters, is an String array. the first
	 *		  parameter should be the token.
	 * @return "ture" or "false" that indicates the valid of input token.
	 * 
	 * */
//	public static boolean main(String[] params)  {
//		boolean result = false;
//		String msg = "Usage: \n" +
//					 "java KBaseAuthValidateToken [input token]\n" ;
//		
//		if (params.length != 1 ) {
//			System.out.println(msg);
//		} else {
//			String testToken = params[0];
//			boolean test_result = false;
//			
//			try {
//				test_result = KBaseAuthValidateToken.verify(testToken);
//			} catch (InvalidKeyException e) {
//				
//				String err_msg = "Error, the Autenticatio server returns an invalid key, " +
//								 "Please check the input token.\n";
//				System.out.println(err_msg);
//				//e.printStackTrace();
//			} catch (NoSuchAlgorithmException e) {
//				// TODO Auto-generated catch block
//				String err_msg = "Error, the system does not support SHA1withRSA algorithm."+
//								 "please check the intalled Java.security package, or JCA.\n";
//				System.out.println(err_msg);
//				//e.printStackTrace();
//			} catch (SignatureException e) {
//				// TODO Auto-generated catch block
//				String err_msg = "Error, the signature is wrong," +
//						 		 "Please check the input token.\n";
//				System.out.println(err_msg);
//				//e.printStackTrace();
//			} catch (RuntimeException e) {
//				// TODO Auto-generated catch block
//				String err_msg = "Error, this is a runtime exception,please check.\n";
//				System.out.println(err_msg);
//				//e.printStackTrace();
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				String err_msg = "Error, can not connect to the Authentication server."+
//								 "Please check the token\n";
//				System.out.println(err_msg);
//				//e.printStackTrace();
//			}
//			
//			if (test_result){
//				result = true;
//				System.out.println("Good token");
//			} else
//				System.out.println("Bad token");
//
//		} //end if	
//		return result;
//	} // end of main()
	
	/**
	 * This function validate the identity of a user by checking the user's token.
	 * The token is a string which is obtained from Authentication server upon the user request.
	 * Once the function have the token, it will parse it and get the information 
	 * this token. The token shall have a HTTPS verification address of Authentication server.
	 * By communicating with the Authentication server, this function will obtain the RSA public key
	 * of the user. This public key is used to verify the Signature data and Signature.
	 *   
	 * 
	 * @param token token is a string used to verify user identity. It concludes:
	 * 				User ID;
	 * 				Client ID;
	 * 				Expiry time;
	 * 				Signature;
	 * 				Authentication server address.
	 *  
	 * @return "true" or "false" indicates the validation of the input token. 
	 * @throws IOException 
	 * @throws NoSuchAlgorithmException 
	 * @throws InvalidKeyException 
	 * @throws SignatureException 
	 * */
	public static boolean verify(String token) throws RuntimeException, IOException, NoSuchAlgorithmException, InvalidKeyException, SignatureException {
		boolean result = false;
		
		if (token == null)
			return result;
		
		/** split the signature_data and signature*/
		int sig_position = token.indexOf("|sig=");
		String sig_data = token.substring(0,sig_position);
		String sig = "";
		if (token.length() - sig_position >= 4)
			token.substring(sig_position+4);  // string start after "="
			
		Hashtable<String, String>  parsed_token = new Hashtable<String, String> ();
		
		/** Parse the Token string */
		if (token != null ) {
			/** parse by "|" */
			String[] token_pairs = sig_data.split("[|]");
			
			for (int i = 0; i < token_pairs.length; i++) {
				/** now parse by "=" */
				String[] single_pair = token_pairs[i].split("[=]");
				/** make sure it is a Key-Value pair */
				if (single_pair.length == 2)
					parsed_token.put(single_pair[0], single_pair[1]);
			}
		}
		
		/** now HTTPS the SigningSubgject of input Token */
		URL validation_url = new URL((String) parsed_token.get("SigningObject"));
		
		/** make the request to Authentication server */
		HttpsURLConnection conn = (HttpsURLConnection) validation_url.openConnection(); 
		InputStream in = conn.getInputStream();
		
		/** Encoding the HTTP response into JSON format */
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		StringBuilder sb = new StringBuilder();
		String line = null;
		
		while ((line = br.readLine()) != null) {
			sb.append((line + "\n"));
		}
		
		DBObject obj = (DBObject)JSON.parse(sb.toString());
		
		/** now get the public key and do the verify */
		RSAPublicKey pubKey = (RSAPublicKey) obj.get("pubkey");
		
		/** http://docs.oracle.com/javase/6/docs/technotes/guides/security/crypto/CryptoSpec.html#KeyFactoryEx */
		Signature s = Signature.getInstance("SHA1withRSA");
		
		s.initVerify(pubKey);
		
		/** update the data
		 * 
		 *	SHA-1 (and all other hashing algorithms) return binary data. That means that (in Java) they produce a byte[].
		 *  That byte array does not represent any specific characters, which means you can't simply turn it into a String 
		 *  like you did.If you need a String, then you have to format that byte[] in a way that can be represented as 
		 *  a String (otherwise, just keep the byte[] around).Two common ways of representing arbitrary byte[] as printable
		 *  characters are BASE64 or simple hex-Strings (i.e. representing each byte by two hexadecimal digits). 
		 *  It looks like you're trying to produce a hex-String. There's also another pitfall: if you want to get the SHA-1 
		 *  of a Java String, then you need to convert that String to a byte[] first (as the input of SHA-1 is a byte[] as well). 
		 *  If you simply use myString.getBytes() as you showed, then it will use the platform default encoding and as such 
		 *  will be dependent on the environment you run it in (for example it could return different data based on the 
		 *  language/locale setting of your OS).A better solution is to specify the encoding to use for the String-to-byte[] 
		 *  conversion like this: myString.getBytes("UTF-8"). Choosing UTF-8 (or another encoding that can represent every 
		 *  unicode character) is the safest choice here.
		 */
		byte[] sig_data_byte = sig_data.getBytes("UTF-8");
		s.update(sig_data_byte);

		/**
		* The equivalent of Perl 's pack "H*", $vartoconvert in Java is :
		* javax.xml.bind.DatatypeConverter.parseHexBinary(hexadecimalString);.
		* For more information on this, I think it is recommended to read 
		* DatatypeConverter class' reference from JavaDocs. 
		*/
		byte[] sig_byte = javax.xml.bind.DatatypeConverter.parseHexBinary(sig);
		
		/** verification of signature*/
		result = s.verify(sig_byte);
	
		return result;
	}; 
	
	/**
	 * this function parse the user name information from a token string.
	 * Assume the user name is put in the beginning of the token string.
	 * 
	 * @param token token is a string used to verify user identity.
	 * @return return the user name which is in a string object.
	 * 
	 * */
	public static String getUserName(String token) {
		String username = "";
		
		int pos_of_first_equal = token.indexOf("=");
		int pos_of_first_vertical_line = token.indexOf("|");
		
		String key = token.substring(0, pos_of_first_equal);
		if (pos_of_first_vertical_line > pos_of_first_equal) {
			String value = token.substring(pos_of_first_equal+1,pos_of_first_vertical_line);
			
			if (key.equals("un"))
				username = value;
		}
			
		return username;
	};
	
	/** 
	 * Check whether the token string has user name "un" key word.
	 * 
	 * @param token token is a string used to verify user identity.
	 * @return true or false that indicates whether the token contain 
	 * 		   user name information.
	 * 
	 * */
	public static boolean validateToken(String token) {
		boolean result = false;
		
		int pos_of_equal_symbol = token.indexOf("=");
		int pos_start = 0;
		if (pos_of_equal_symbol != -1) {
			String key = token.substring(pos_start,pos_of_equal_symbol);
			if (key.equals("un")) {
				result = true;
			} else {
				pos_start = pos_of_equal_symbol;
				pos_of_equal_symbol = token.indexOf("=",pos_start+1);
			}
		}
		return result;
	}
};
