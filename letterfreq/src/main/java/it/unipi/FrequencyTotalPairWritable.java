package it.unipi;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FrequencyTotalPairWritable implements Writable {
    private double frequency;
    private int total;
    
    public FrequencyTotalPairWritable(double frequency, int total) {
        this.frequency = frequency;
        this.total = total;
    }

    public FrequencyTotalPairWritable() {
        this(0.0, 0);
    }

    public double getFrequency() {
        return frequency;
    }

    public int getTotal() {
        return total;
    }

    public void set(double frequency, int total) {
        this.frequency = frequency;
        this.total = total;
    }

    public void inPlaceAverage(
        FrequencyTotalPairWritable other) 
    {
        if (total == 0 && other.total == 0) {
            frequency = 0.0;
            total = 0;
        } else {
            frequency = (frequency * (double)total + other.frequency * (double)other.total) / 
                (total + other.total);
            total += other.total;
        }
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
        frequency = arg0.readDouble();
        total = arg0.readInt();
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
        arg0.writeDouble(frequency);
        arg0.writeInt(total);
    }
    
    @Override
    public String toString() {
        return frequency + "\t" + total;
    }
}
