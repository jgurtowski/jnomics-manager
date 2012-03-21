package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsMapper;
import edu.cshl.schatz.jnomics.ob.SAMRecordWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SamtoolsMap extends JnomicsMapper<SAMRecordWritable,NullWritable, SamtoolsMap.SamtoolsKey, SAMRecordWritable> {

    public static final int DEFAULT_GENOME_BINSIZE = 1000000;
    
    private final SamtoolsKey stkey = new SamtoolsKey();
    private int binsize;

    public static final JnomicsArgument genome_binsize_arg = new JnomicsArgument("genome_binsize",
            "Bin Size to Break up the Genome", false);
        
    
    @Override
    public Class getOutputKeyClass() {
        return SamtoolsMap.SamtoolsKey.class;
    }

    @Override
    public Class getOutputValueClass() {
        return SAMRecordWritable.class;
    }

    @Override
    public JnomicsArgument[] getArgs() {
        return new JnomicsArgument[]{genome_binsize_arg};
    }

    public static class SamtoolsKey implements WritableComparable<SamtoolsKey> {

        private Text ref = new Text();
        private IntWritable bin = new IntWritable();
        private IntWritable position = new IntWritable();

        public SamtoolsKey(){}

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            ref.write(dataOutput);
            bin.write(dataOutput);
            position.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            ref.readFields(dataInput);
            bin.readFields(dataInput);
            position.readFields(dataInput);
        }

        @Override
        public int compareTo(SamtoolsKey o) {
            int diff;
            if((diff = ref.compareTo(o.getRef())) != 0)return diff;
            if((diff = bin.compareTo(o.getBin())) != 0)return diff;
            return position.compareTo(o.getPosition());
        }

        public String toString(){
            return ref.toString() +"-" + bin.toString() + "-" +position.toString();
        }

        public void setRef(String ref){
            this.ref.set(ref);
        }

        public void setBin(int bin){
            this.bin.set(bin);
        }

        public void setPosition(int position){
            this.position.set(position);
        }

        public void setRef(Text ref){
            this.ref.set(ref);
        }

        public Text getRef() {
            return ref;
        }

        public IntWritable getBin() {
            return bin;
        }

        public IntWritable getPosition() {
            return position;
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String binsize_str = conf.get(genome_binsize_arg.getName());
        binsize = binsize_str == null ? DEFAULT_GENOME_BINSIZE  : Integer.parseInt(binsize_str);
    }

    @Override
    protected void map(SAMRecordWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
        if(key.getMappingQuality().get() == 0) // remove unmapped reads
            return;
        int alignmentStart = key.getAlignmentStart().get();
        stkey.setPosition(alignmentStart);
        stkey.setRef(key.getReferenceName());
        int bin = alignmentStart / binsize;
        stkey.setBin(bin);
        context.write(stkey, key);
        int end = alignmentStart + key.getReadString().toString().length() - 1;
        if(end >= (bin+1) * binsize){
            stkey.setBin(bin+1);
            context.write(stkey,key);
        }
    }
}