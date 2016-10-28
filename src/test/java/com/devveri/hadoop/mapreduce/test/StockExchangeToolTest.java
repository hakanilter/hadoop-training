package com.devveri.hadoop.mapreduce.test;

import com.devveri.hadoop.mapreduce.tool.StockExchangeTool;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static com.devveri.hadoop.mapreduce.util.IOUtil.delete;

public class StockExchangeToolTest {

    private static final String INPUT = "src/test/resources/data/nyse";
    private static final String OUTPUT = "/tmp/mapreduce-nyse";

    @Before
    public void setUp() throws Exception {
        delete(new File(OUTPUT));
    }

    @Test
    public void test() throws Exception {
        // run the job
        StockExchangeTool tool = new StockExchangeTool();
        tool.run(new String[] {INPUT, OUTPUT});
    }

}
