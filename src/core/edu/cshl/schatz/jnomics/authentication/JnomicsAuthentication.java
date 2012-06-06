package edu.cshl.schatz.jnomics.authentication;

import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.server.AuthenticationHandler;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;

import javax.servlet.ServletException;
import java.io.IOException;
import java.util.Properties;

/**
 * User: james
 */
public class JnomicsAuthentication implements AuthenticationHandler{

    private static final String TYPE = "jnomics";
    
    Properties props;
    
    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public void init(Properties properties) throws ServletException {
        props = properties;
    }

    @Override
    public void destroy() {

    }

    @Override
    public AuthenticationToken authenticate(javax.servlet.http.HttpServletRequest httpServletRequest,
                                            javax.servlet.http.HttpServletResponse httpServletResponse)
            throws IOException, AuthenticationException {
        String username = httpServletRequest.getParameter("user.name");
        if(null == username)
            throw new AuthenticationException("Anonymous requests are disallowed");
        return 0 == username.compareTo("ralf") ? new AuthenticationToken(username,username,TYPE) : null;
    }
}
