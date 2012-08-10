package edu.cshl.schatz.jnomics.test.core;

import edu.cshl.schatz.jnomics.ob.FixedKmerWritable;

/**
 * User: james
 */
public class KmerAndCount implements Comparable<KmerAndCount> {
    FixedKmerWritable kmer;
    int count;

    public KmerAndCount(FixedKmerWritable kmer, int count) throws Exception {
        this.kmer = kmer;
        this.count = count;
    }

    @Override
    public int compareTo(KmerAndCount o) {
        return kmer.compareTo(o.getKmer());
    }

    @Override
    public int hashCode() {
        return kmer.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if(!(o instanceof KmerAndCount))
            return false;
        return kmer.equals(((KmerAndCount) o).getKmer());
    }

    public FixedKmerWritable getKmer() {
        return kmer;
    }

    public int getCount() {
        return count;
    }
    public String toString(){
        return new String(kmer.get()) + ":" + count;
    }
}
