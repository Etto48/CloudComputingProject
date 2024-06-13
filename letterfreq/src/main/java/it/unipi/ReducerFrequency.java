package it.unipi;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

public class ReducerFrequency extends org.apache.hadoop.mapreduce.Reducer<
    Char,
    LongWritable,
    Char,
    DoubleWritable> 
{
    private long total;

    @Override
    public void setup(Context context) {
        total = context.getConfiguration().getLong("total", 0);
        if (total == 0) {
            throw new RuntimeException("Total count is zero");
        }
    }

    private final DoubleWritable result = new DoubleWritable();
    public void reduce(Char key, Iterable<LongWritable> values, Context context) 
        throws IOException, InterruptedException 
    { 
        long sum = 0;
        for (LongWritable val : values) { 
            sum += val.get();
        }
        result.set((double)sum / total);
        context.write(key, result);
    } 
}
