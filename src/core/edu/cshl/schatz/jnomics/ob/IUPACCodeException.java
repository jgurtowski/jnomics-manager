/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.ob;

/**
 * @author Matthew Titmus
 */
public class IUPACCodeException extends RuntimeException {
    private static final long serialVersionUID = 5225698475128327024L;

    /**
	 * 
	 */
    public IUPACCodeException() {}

    /**
     * @param message
     */
    public IUPACCodeException(String message) {
        super(message);
    }

    /**
     * @param message
     * @param cause
     */
    public IUPACCodeException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * @param cause
     */
    public IUPACCodeException(Throwable cause) {
        super(cause);
    }
}
