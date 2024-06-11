package it.unipi;

import java.io.IOException;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<
    Char,
    CountTotalPairWritable,
    Char,
    CountTotalPairWritable> 
{ 
    private final CountTotalPairWritable result = new CountTotalPairWritable();
    public void reduce(Char key, Iterable<CountTotalPairWritable> values, Context context) 
        throws IOException, InterruptedException { 
        result.set(0, 0);
        for (CountTotalPairWritable val : values) { 
            result.inPlaceSum(val);
        }
        context.write(key, result); 
    } 
}
