package com.devveri.hadoop.mapreduce.tool;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReduceSideJoinTool extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.printf("Usage: %s [generic options] <input> <output> <map-field> <reduce-field>\n", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = new Job();
        job.setJarByClass(ReduceSideJoinTool.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(ReduceSideJoinToolMapper.class);
        job.setReducerClass(ReduceSideJoinToolReducer.class);

        job.getConfiguration().set("field1", args[2]);
        job.getConfiguration().set("field2", args[3]);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ReduceSideJoinTool(), args);
        System.exit(exitCode);
    }

    /**
     * Mapper
     */
    static class ReduceSideJoinToolMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Gson gson = new Gson();

        private String field1;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            field1 = context.getConfiguration().get("field1");
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            Map<String, Object> map = gson.fromJson(line, new TypeToken<Map<String, Object>>(){}.getType());

            if (map.containsKey(field1)) {
                String userId = (String) map.get(field1);
                if (userId != null) {
                    context.write(new Text(userId), value);
                }
            }
        }

    }

    /**
     * Reducer
     */
    static class ReduceSideJoinToolReducer extends Reducer<Text, Text, NullWritable, Text> {

        private Gson gson = new Gson();

        private String field2;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            field2 = context.getConfiguration().get("field2");
        }

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Map<String, Object> parent = null;
            List<Map<String, Object>> children = new ArrayList<>();

            // find parent and child records
            for (Text value : values) {
                String line = value.toString();
                Map<String, Object> map = gson.fromJson(line, new TypeToken<Map<String, Object>>(){}.getType());
                if (map.containsKey(field2)) {
                    children.add(map);
                } else {
                    parent = map;
                }
            }

            // join
            if (parent != null) {
                for (Map<String, Object> child : children) {
                    child.putAll(parent);
                    String json = gson.toJson(child);
                    context.write(NullWritable.get(), new Text(json));
                }
            }
        }

    }

}