package edu.cshl.schatz.jnomics.util;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

/**
 * User: james
 */
public class FileUtil {
    
    final static Log LOG = LogFactory.getLog(FileUtil.class);

    public static InputStream getInputStreamWrapperFromExtension(InputStream inStream, String extension) throws IOException {
        if(extension.compareTo(".gz") == 0){
            return new GZIPInputStream(inStream);
        }else if(extension.compareTo(".bz2") == 0){
            return new BZip2CompressorInputStream(inStream);
        }else if(extension.compareTo(".txt") == 0){
            return inStream;
        }

        LOG.warn("Unknown Extension returning original InputStream");
        return inStream;
    }
    
    public static String getExtension(String name){
        return name.substring(name.lastIndexOf("."));
    }
}
