package it.unipi;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<
    Char,
    LongWritable,
    Char,
    LongWritable> 
{ 
    private final LongWritable result = new LongWritable();
    public void reduce(Char key, Iterable<LongWritable> values, Context context) 
        throws IOException, InterruptedException { 
        long sum = 0;
        for (LongWritable val : values) { 
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result); 
    } 
}
