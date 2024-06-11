package it.unipi;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class AppTest 
{
    @Test
    public void frequencyTotalPairWritableAverage() {
        CountTotalPairWritable c1 = new CountTotalPairWritable(5, 10);
        CountTotalPairWritable c2 = new CountTotalPairWritable(10, 20);
        c1.inPlaceSum(c2);
        assertTrue(c1.getCount() == 15);
        assertTrue(c1.getTotal() == 30);
    }
}
