package com.devveri.hive.udf;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.text.DateFormat;
import java.util.Date;

public class UnixTimeToDateUDF {

    public Text evaluate(Text value) {
        if (value == null) return null;
        long timestamp = Long.parseLong(value.toString());
        return new Text(toDate(timestamp));
    }

    public Text evaluate(LongWritable value) {
        if (value == null) return null;
        long timestamp = value.get();
        return new Text(toDate(timestamp));
    }

    private String toDate(long timestamp) {
        Date date = new Date (timestamp * 1000);
        return DateFormat.getInstance().
                format(date).toString();
    }

}
