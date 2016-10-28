package com.devveri.hadoop.mapreduce.test;

import com.devveri.hadoop.mapreduce.tool.NASAParseTool;
import com.devveri.hadoop.mapreduce.tool.WordCount;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.Map;

import static com.devveri.hadoop.mapreduce.util.IOUtil.*;
import static org.junit.Assert.*;

public class NASAParseToolTest {

    private static final String INPUT = "src/test/resources/data/nasa";
    private static final String OUTPUT = "/tmp/mapreduce-nasa";
    private static final String OUTPUT_FILE = OUTPUT + "/part-r-00000";

    @Before
    public void setUp() throws Exception {
        delete(new File(OUTPUT));
    }

    @Test
    public void test() throws Exception {
        // run the job
        NASAParseTool tool = new NASAParseTool();
        tool.run(new String[] {INPUT, OUTPUT});

        // assert output
        String output = readFile(new File(OUTPUT_FILE));
        Map<String, String> map = parseOutput(output);
        assertEquals("2", map.get("arf.dut.com.tr"));
    }

}
