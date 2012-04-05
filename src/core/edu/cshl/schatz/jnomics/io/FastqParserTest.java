package edu.cshl.schatz.jnomics.io;

import junit.framework.TestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * User: james
 */
public class FastqParserTest extends TestCase {

    private List<String []> fastqTestStrs;
    private FastqParser parser;

    public static String strJoin(String []arr, String delim){
        StringBuilder builder = new StringBuilder();
        for(int i=0; i< arr.length; i++){
            if(i != 0){
                builder.append(delim);
            }
            builder.append(arr[i]);
        }
        return builder.toString();
    }
    
    @Override
    protected void setUp() throws Exception {
        fastqTestStrs =  new ArrayList<String[]>();
        fastqTestStrs.add(new String[]{"@seq1","ACGTACGT","+sequence","22222222"});
        fastqTestStrs.add(new String[]{"@seq2","GGGGGGGG","+SEQ","22222222"});
    }

    public void testParse() throws IOException {
        for(String[] arr : fastqTestStrs){
            parser = new FastqParser(new ByteArrayInputStream(strJoin(arr,"\n").getBytes()));
            for( FastqParser.FastqRecord record: parser){
                assertEquals(record.getName(), arr[0].replaceFirst("@",""));
                assertEquals(record.getSequence(), arr[1]);
                assertEquals(record.getDescription(), arr[2].replaceFirst("[+]",""));
                assertEquals(record.getQuality(), arr[3]);
            }
            parser.close();
        }
    }
}
