package com.devveri.hadoop.mapreduce.tool;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * For final sort use:
 *   $ less mapreduce-apache/part-r-00000 |sort -n -r | less
 */
public class ApacheLogParseTool extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = new Job();
        job.setJarByClass(ApacheLogParseTool.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(ApacheLogParseToolMapper.class);
        job.setReducerClass(ApacheLogParseToolReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ApacheLogParseTool(), args);
        System.exit(exitCode);
    }

    /**
     * Mapper
     */
    static class ApacheLogParseToolMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final String PATTERN = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\" (\\S+)";

        private Pattern pattern;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            pattern = Pattern.compile(PATTERN);
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();

            Matcher matcher = pattern.matcher(line);
            if (!matcher.matches()) {
                //System.err.println("invalid log pattern: " + line);
                return;
            }

            // parse data
            String ipAddress = matcher.group(1);
            String dateTime = matcher.group(4);
            String request = matcher.group(5);
            String response = matcher.group(6);
            String bytes = matcher.group(7);
            String browser = matcher.group(9);
            String referer = null;
            if (!matcher.group(8).equals("-")) {
                referer = matcher.group(8);
            }

            if (referer != null) {
                context.write(new Text(browser), new IntWritable(1));
            }
        }

    }

    /**
     * Reducer
     */
    static class ApacheLogParseToolReducer extends Reducer<Text, IntWritable, IntWritable, Text> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(new IntWritable(sum), key);
        }

    }

}