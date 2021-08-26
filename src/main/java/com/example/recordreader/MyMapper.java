package com.example.recordreader;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<IntWritable, Text, Text, IntWritable> {
    @Override
    protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.err.printf("<%s,%s>\n", key, value);
        context.write(value, new IntWritable(1));
    }
}
