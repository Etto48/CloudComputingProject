package it.unipi;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class AppTest 
{
    @Test
    public void frequencyTotalPairWritableAverage() {
        FrequencyTotalPairWritable f1 = new FrequencyTotalPairWritable(0.5, 10);
        FrequencyTotalPairWritable f2 = new FrequencyTotalPairWritable(0.3, 20);
        f1.inPlaceAverage(f2);
        assertTrue(Math.abs(f1.getFrequency() - 0.3666666667) < 0.0000001);
        assertTrue(f1.getTotal() == 30);
    }
}
