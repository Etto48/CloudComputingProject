package it.unipi;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CountTotalPairWritable implements Writable {
    private long count;
    private long total;
    
    public CountTotalPairWritable(long count, long total) {
        this.count = count;
        this.total = total;
    }

    public CountTotalPairWritable() {
        this(0, 0);
    }

    public long getCount() {
        return count;
    }

    public long getTotal() {
        return total;
    }

    public void set(long count, long total) {
        this.count = count;
        this.total = total;
    }

    public void inPlaceSum(
        CountTotalPairWritable other) 
    {
        count += other.count;
        total += other.total;
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
        count = arg0.readLong();
        total = arg0.readLong();
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
        arg0.writeLong(count);
        arg0.writeLong(total);
    }
    
    @Override
    public String toString() {
        double frequency = (double) count / total;
        return frequency + "\t" + count;
    }
}
