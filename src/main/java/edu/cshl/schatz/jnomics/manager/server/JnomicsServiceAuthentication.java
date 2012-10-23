package edu.cshl.schatz.jnomics.manager.server;

import edu.cshl.schatz.jnomics.authentication.KBaseAuthValidateToken;
import edu.cshl.schatz.jnomics.manager.api.Authentication;

import java.util.HashMap;
import java.util.Map;

/**
 * User: james
 * Provides caching mechanism so globus service is not overloaded
 */
public class JnomicsServiceAuthentication {

    private static class AuthContainer {
        private static int TIMEOUT = 60* 1000 * 10;
        private String username;
        private long expire;

        public AuthContainer(String username){
            this.username = username;
            expire = System.currentTimeMillis() + TIMEOUT;
        }
        
        public String getUsername() {
            return username;
        }

        public boolean isExpired(){
            return System.currentTimeMillis() < expire ? false : true;
        }
    }

    //key: kbase token, value: authentication session metadata
    private Map<String, AuthContainer> tokenCache = new HashMap<String, AuthContainer>();

    /**
     *
     * @param auth takes authentication object from service
     *             Kbase (globus online) token required
     * @return The username if authenticated, null if authentication fails
     */
    public String authenticate(Authentication auth){
        String token = auth.getToken();
        AuthContainer container;
        if(null != token){
            if(tokenCache.containsKey(token)){
                container = tokenCache.get(token);
                if(!container.isExpired())
                    return tokenCache.get(token).getUsername();
                else
                    tokenCache.remove(token);
            }
            try {
                if(KBaseAuthValidateToken.verify(token)){
                    String username = KBaseAuthValidateToken.getUserName(token);
                    if(null != username){
                        tokenCache.put(token,new AuthContainer(username));
                        return username;
                    }
                }
            } catch (Exception e){
                return null;
            }
        }
        return null;
    }
}
