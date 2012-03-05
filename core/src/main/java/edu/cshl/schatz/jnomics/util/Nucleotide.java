package edu.cshl.schatz.jnomics.util;

public class Nucleotide {

    /*
    * For encoding Nucleotide letters into bits
    * A = 00000000
    * C = 01000000
    * G = 10000000
    * T = 11000000
    * These can be or'd with an existing byte to pack them into the byte
    */
    public static final byte[] charMap = new byte[]{0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,(1<<6),
            0,0,0,(byte) (1<<7),0,0,0,0,0,0,0,0,0,0,0,0,(byte) (3<<6)};

    public static final char[] reverseCharMap = new char[]{'A','C','G','T'};


    public static byte[] reverseComplement(byte[] in, int start, int end){
        byte[] n = new byte[end-start];
        for(int i =start; i< end;i++){
            n[(end-start) - 1 - (i-start)] = (byte) Nucleotide.reverseCharMap[(((byte)3<<6) ^ Nucleotide.charMap[in[i]] & 0xff) >>> 6];
        }
        return n;
    }

    public static String reverseComplement(String in){
        return new String(reverseComplement(in.getBytes(),0,in.length()));
    }


}
