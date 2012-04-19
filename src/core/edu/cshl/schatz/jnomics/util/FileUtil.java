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
    
    public static InputStream getInputStreamFromExtension(File file) throws IOException {
        String ext = file.getName().substring(file.getName().lastIndexOf("."));
        FileInputStream fis = new FileInputStream(file);
        
        if(ext.compareTo(".gz") == 0){
            return new GZIPInputStream(fis);
        }else if(ext.compareTo(".bz2") == 0){
            return new BZip2CompressorInputStream(fis);
        }else if(ext.compareTo(".txt") == 0){
            return fis;
        }

        LOG.warn("Unknown Extension returning FileInputStream");
        return fis;
    }
}
