package com.devveri.hive.udf.test;

import com.devveri.hive.udf.UnixTimeToDateUDF;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class UnixTimeToDateUDFTest {

    private static final long TIME = 1477046130709L;
    private UnixTimeToDateUDF udf;

    @Before
    public void setUp() {
        udf = new UnixTimeToDateUDF();
    }

    @Test
    public void testText() {
        Text result = udf.evaluate(new Text(Long.toString(TIME)));
        assertEquals("9/24/75 10:51 AM", result.toString());
    }

    @Test
    public void testLongWritable() {
        Text result = udf.evaluate(new LongWritable(TIME));
        assertEquals("9/24/75 10:51 AM", result.toString());
    }

}
