package com.devveri.hadoop.mapreduce.test;

import com.devveri.hadoop.mapreduce.tool.ApacheLogParseTool;
import com.devveri.hadoop.mapreduce.tool.NASAParseTool;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Map;

import static com.devveri.hadoop.mapreduce.util.IOUtil.*;
import static org.junit.Assert.assertEquals;

public class ApacheLogParseToolTest {

    private static final String INPUT = "src/test/resources/data/apache";
    private static final String OUTPUT = "/tmp/mapreduce-apache";

    @Before
    public void setUp() throws Exception {
        delete(new File(OUTPUT));
    }

    @Test
    public void test() throws Exception {
        // run the job
        ApacheLogParseTool tool = new ApacheLogParseTool();
        tool.run(new String[] {INPUT, OUTPUT});
    }

}
