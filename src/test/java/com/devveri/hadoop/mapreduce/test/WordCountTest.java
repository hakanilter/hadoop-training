package com.devveri.hadoop.mapreduce.test;

import static com.devveri.hadoop.mapreduce.util.IOUtil.readFile;
import static com.devveri.hadoop.mapreduce.util.IOUtil.parseOutput;
import static com.devveri.hadoop.mapreduce.util.IOUtil.delete;

import static org.junit.Assert.*;

import com.devveri.hadoop.mapreduce.tool.WordCount;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.Map;

@Ignore
public class WordCountTest {

    private static final String HELLO_TEXT = "src/test/resources/word_count.txt";
    public static final String SHAKESPEARE = "src/test/resources/shakespeare.txt";

    private static final String OUTPUT = "/tmp/mapreduce-wordcount";
    private static final String OUTPUT_FILE = OUTPUT + "/part-r-00000";

    @Before
    public void setUp() throws Exception {
        delete(new File(OUTPUT));
    }

    @Test
    public void testWordCount() throws Exception {
        // run the job
        WordCount tool = new WordCount();
        tool.run(new String[] {HELLO_TEXT, OUTPUT});

        // assert output
        String output = readFile(new File(OUTPUT_FILE));
        Map<String, String> map = parseOutput(output);
        assertEquals("1", map.get("hadoop"));
        assertEquals("2", map.get("hello"));
        assertEquals("1", map.get("mapreduce"));
    }

    @Test
    public void testShakespeare() throws Exception {
        // run the job
        WordCount wordCount = new WordCount();
        wordCount.run(new String[] {SHAKESPEARE, OUTPUT});

        // assert output
        String output = readFile(new File(OUTPUT_FILE));
        assertNotNull(output);
        assertTrue(output.length() > 0);
    }

    @Test
    public void testWordCountWithoutStopWords() throws Exception {
        // run the job
        WordCount wordCount = new WordCount();
        wordCount.run(new String[] {HELLO_TEXT, OUTPUT});

        // assert output
        String output = readFile(new File(OUTPUT_FILE));
        Map<String, String> map = parseOutput(output);
        assertEquals("1", map.get("hadoop"));
        assertEquals("2", map.get("hello"));
        assertEquals("1", map.get("mapreduce"));
    }

}
