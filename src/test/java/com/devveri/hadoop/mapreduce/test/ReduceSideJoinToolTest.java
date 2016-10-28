package com.devveri.hadoop.mapreduce.test;

import com.devveri.hadoop.mapreduce.tool.ReduceSideJoinTool;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Map;

import static com.devveri.hadoop.mapreduce.util.IOUtil.delete;
import static com.devveri.hadoop.mapreduce.util.IOUtil.parseOutput;
import static com.devveri.hadoop.mapreduce.util.IOUtil.readFile;

public class ReduceSideJoinToolTest {

    private static final String INPUT = "src/test/resources/data/relational";
    private static final String OUTPUT = "/tmp/mapreduce-join";
    private static final String OUTPUT_FILE = OUTPUT + "/part-r-00000";

    @Before
    public void setUp() throws Exception {
        delete(new File(OUTPUT));
    }

    @Test
    public void test() throws Exception {
        // run the job
        ReduceSideJoinTool tool = new ReduceSideJoinTool();
        tool.run(new String[] {INPUT, OUTPUT, "user_id", "address_id"});

        // assert output
        String output = readFile(new File(OUTPUT_FILE));
        Map<String, String> map = parseOutput(output);
    }

}
