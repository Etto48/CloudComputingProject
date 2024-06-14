package it.unipi;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class MapperNoCombiner extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Char, LongWritable> {
    private final Char key = new Char();
    private final LongWritable one = new LongWritable(1);

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException { 
        long sum = 0;
        String normalizedInput = StringUtils.stripAccents(value.toString());
        for (int i = 0; i < normalizedInput.length(); i++) {
            int lowerC = Character.toLowerCase(normalizedInput.charAt(i));
            if (lowerC >= 'a' && lowerC <= 'z') {
                this.key.set((char)lowerC);
                context.write(this.key, this.one);
                sum++;
            }
        };
        context.getCounter("LetterFreq", "total").increment(sum);
    }
}
