package it.unipi;

import java.io.IOException;

import org.apache.hadoop.io.Text;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<
    Text,
    FrequencyTotalPairWritable,
    Text,
    FrequencyTotalPairWritable> 
{ 
    private final FrequencyTotalPairWritable result = new FrequencyTotalPairWritable();
    public void reduce(Text key, Iterable<FrequencyTotalPairWritable> values, Context context) 
        throws IOException, InterruptedException { 
        result.set(0.0, 0);
        for (FrequencyTotalPairWritable val : values) { 
            result.inPlaceAverage(val);
        }
        context.write(key, result); 
    } 
}
