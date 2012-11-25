package edu.cshl.schatz.jnomics.manager.common;

/**
 * User: james
 */

public class KBaseIDTranslator {

    /**
     * Translates Kbase ID's (containing pipe)
     * into representation on HDFS with a '_'
     * If the ID does not look like a kbase ID
     * it is returned unmodified
     * @param id ID to translate
     * @return Translated ID
     */
    public static String translate(String id){
        if(!id.startsWith("kb|"))
            return id;
        return id.replace('|','_');
    }
}
