package edu.cshl.schatz.jnomics.ob;

import edu.cshl.schatz.jnomics.util.TextUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * User: james
 */
public class ReadWritable implements WritableComparable<ReadWritable>{

    private Text name, description, sequence, quality;
    
    
    public ReadWritable(){
        name = new Text();
        sequence = new Text();
        quality = new Text();
        description = new Text();
    }

    /**
     * Copy constructor
     * @param other object to copy
     */
    public ReadWritable(ReadWritable other){
        this();
        name.set(other.getName());
        sequence.set(other.getSequence());
        quality.set(other.getQuality());
        description.set(other.getDescription());
    }

    public Text getDescription(){
        return description;
    }

    public Text getName() {
        return name;
    }

    public Text getSequence() {
        return sequence;
    }

    public Text getQuality() {
        return quality;
    }

    /**
     * Contains trailing linesep
     * @return String with fastq view (has trailing newline)
     */
    public String getFastqString(){
        return TextUtil.join(new String[]{name.toString(),
                sequence.toString(),
                description.toString(),
                quality.toString()}, System.getProperty("line.separator"));
    }

    @Override
    public int compareTo(ReadWritable o) {
        int diff;
        if((diff = name.compareTo(o.getName()) )!= 0) return diff;
        if((diff = description.compareTo(o.getDescription()) )!= 0) return diff;
        if((diff = sequence.compareTo(o.getSequence()) )!= 0) return diff;
        if((diff = quality.compareTo(o.getQuality()) )!= 0) return diff;
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        name.write(dataOutput);
        sequence.write(dataOutput);
        quality.write(dataOutput);
        description.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        name.readFields(dataInput);
        sequence.readFields(dataInput);
        quality.readFields(dataInput);
        description.readFields(dataInput);
    }
    
    public String toString(){
        return name.toString()+","+sequence.toString();
    }
}
