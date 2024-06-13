package it.unipi;

import java.io.IOException;
import java.util.Vector;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Char, LongWritable> {
    private final static int LETTER_COUNT = 26; 
    private final Vector<Integer> combinerCounters = new Vector<Integer>();
    private long sum = 0;
    private final Char key = new Char();
    private final LongWritable value = new LongWritable();

    @Override
    public void setup(Context context) {
        sum = 0;
        combinerCounters.clear();
        for (int i = 0; i < LETTER_COUNT; i++) {
            combinerCounters.add(0);
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for (int i = 0; i < LETTER_COUNT; i++) {
            key.set((char)('a' + i));
            value.set(combinerCounters.get(i));
            context.write(key, value);
        }
        key.set('*');
        value.set(sum);
        context.write(key, value);
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException { 
        StringUtils.stripAccents(value.toString()).chars().forEach(c -> {
            int lowerC = Character.toLowerCase(c);
            if (lowerC >= 'a' && lowerC <= 'z') {
                int index = lowerC - 'a';
                combinerCounters.set(index, combinerCounters.get(index) + 1);
                sum++;
            }
        });
    }
}
