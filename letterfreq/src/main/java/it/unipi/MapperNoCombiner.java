package it.unipi;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class MapperNoCombiner extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Char, LongWritable> {
    private final Char key = new Char();
    private final LongWritable value = new LongWritable(1);

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException { 
        long sum = 0;
        for (int c : StringUtils.stripAccents(value.toString()).chars().toArray()) {
            int lowerC = Character.toLowerCase(c);
            if (lowerC >= 'a' && lowerC <= 'z') {
                this.key.set((char)lowerC);
                context.write(this.key, this.value);
                sum++;
            }
        };
        this.key.set('*');
        this.value.set(sum);
        context.write(this.key, this.value);
    }
}
