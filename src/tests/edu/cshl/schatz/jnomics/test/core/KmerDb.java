package edu.cshl.schatz.jnomics.test.core;

import edu.cshl.schatz.jnomics.ob.FixedKmerWritable;

import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.util.*;

/**
 * User: james
 * Test out Memory mapped kmer database
 */
public class KmerDb implements List<KmerAndCount>, RandomAccess {

    private MappedByteBuffer mmapdb;
    private int size;
    private int ksize;
    private byte[] kmerBuffer;
    private int countBuffer;
    
    public static void main(String []args) throws Exception {
     /*   File sampleDb = new File("sampleDb");
        int kmersize = 23;
        DataOutputStream stream = new DataOutputStream(new FileOutputStream(sampleDb));
        stream.writeInt(kmersize);
        stream.writeInt(3);
        byte []encoded = new byte[kmersize / 4 + 1];
        FixedKmerWritable.encodePackedBytes("ACCTGTGACGTAGTGGAGTGGGG".getBytes(), encoded);
        stream.write(encoded);
        stream.writeInt(222);
        FixedKmerWritable.encodePackedBytes("ACGTGTTACGTCGTGGGGGGGCG".getBytes(), encoded);
        stream.write(encoded);
        stream.writeInt(30);
        FixedKmerWritable.encodePackedBytes("ACGTGTTACGTCGTGGGGGGGCG".getBytes(), encoded);
        stream.write(encoded);
        stream.writeInt(10);
        stream.close();

        KmerDb db = KmerDb.open(sampleDb);
        FixedKmerWritable kw = new FixedKmerWritable(23);
        kw.set("ACGTGTTACGTCGTGGGGGGGCG");
        KmerAndCount test =  new KmerAndCount(kw,0);
        System.out.println(db.contains(test));
        System.out.println(db.get(test));
        db.close();*/
    }

    public void close(){
        mmapdb.clear();
    }
    
    private KmerDb(MappedByteBuffer byteBuffer){
        mmapdb = byteBuffer;
        ksize = mmapdb.getInt(0);
        size = mmapdb.getInt(4);
        kmerBuffer = new byte[ksize];
    }
    
    public static KmerDb open(File db) throws IOException {
        MappedByteBuffer mmapdb = new RandomAccessFile(db,"r").
                getChannel().map(FileChannel.MapMode.READ_ONLY,0,db.length());
        return new KmerDb(mmapdb);
    }
    
    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        if(size < 1)
            return true;
        return false;
    }

    @Override
    public boolean contains(Object o) {
        if(indexOf(o) > 0)
            return true;
        return false;
    }

    @Override
    public Iterator<KmerAndCount> iterator() {
        return null;
    }

    @Override
    public Object[] toArray() {
        return new Object[0];
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return null;
    }

    @Override
    public boolean add(KmerAndCount kmerAndCount) {
        return false;
    }

    @Override
    public boolean remove(Object o) {
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for(Object i : c){
            if(!this.contains(i))
                return false;
        }
        return true;
    }

    @Override
    public boolean addAll(Collection<? extends KmerAndCount> c) {
        return false;
    }

    @Override
    public boolean addAll(int index, Collection<? extends KmerAndCount> c) {
        return false;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return false;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return false;
    }

    @Override
    public void clear() {
    }

    @Override
    public KmerAndCount get(int index) {
        /*if(index > size || index < 0)
            return null;

        int offset = 2 * 4; // header is 2 ints
        offset += index * ((ksize / 4 + 1) + 4); //kmer + count
        mmapdb.position(offset);
        mmapdb.get(kmerBuffer);
        countBuffer = mmapdb.getInt();
        FixedKmerWritable w = new FixedKmerWritable(ksize);
        w.setAlreadyEncoded(kmerBuffer);
        try {
            return new KmerAndCount(w,countBuffer);
        } catch (Exception e) {
            return null;
        } */
        return null;
    }

    public KmerAndCount get(Object o){
        int idx = indexOf(o);
        if(idx > 0)
            return get(idx);
        return null;
    }
    
    @Override
    public KmerAndCount set(int index, KmerAndCount element) {
        return null;
    }

    @Override
    public void add(int index, KmerAndCount element) {
    }

    @Override
    public KmerAndCount remove(int index) {
        return null;
    }

    @Override
    public int indexOf(Object o) {
        if(!(o instanceof KmerAndCount))
            return -1;
        return Collections.binarySearch(this,(KmerAndCount)o);
    }

    @Override
    public int lastIndexOf(Object o) {
        return indexOf(o);
    }

    @Override
    public ListIterator<KmerAndCount> listIterator() {
        return null;
    }

    @Override
    public ListIterator<KmerAndCount> listIterator(int index) {
        return null;
    }

    @Override
    public List<KmerAndCount> subList(int fromIndex, int toIndex) {
        return null;
    }
}
