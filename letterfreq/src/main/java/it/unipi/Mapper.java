package it.unipi;

import java.io.IOException;
import java.util.Vector;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, FrequencyTotalPairWritable> {
    private final static int LETTER_COUNT = 26; 
    private final Vector<Integer> values = new Vector<Integer>();

    @Override
    public void setup(Context context) {
        values.clear();
        // the last one is for the total letter count
        for (int i = 0; i < LETTER_COUNT + 1; i++) {
            values.add(0);
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        int total = values.get(LETTER_COUNT);
        for (int i = 0; i < LETTER_COUNT; i++) {
            Text key = new Text(Character.toString((char) ('a' + i)));
            double frequency = (double) values.get(i) / total;
            context.write(key, new FrequencyTotalPairWritable(frequency, total));
        }
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException { 
        StringUtils.stripAccents(value.toString()).chars().forEach(c -> {
            int lowerC = Character.toLowerCase(c);
            if (lowerC >= 'a' && lowerC <= 'z') {
                int index = lowerC - 'a';
                values.set(index, values.get(index) + 1);
                values.set(LETTER_COUNT, values.get(LETTER_COUNT) + 1);
            }
        });
    }
}