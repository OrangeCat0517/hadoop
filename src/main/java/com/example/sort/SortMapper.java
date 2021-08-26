package com.example.sort;

import org.apache.commons.beanutils.converters.BigIntegerConverter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    private static final IntWritable num = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        num.set(Integer.parseInt(line));
        //把等待排序的元素读入，变成map输出阶段的key
        context.write(num, new IntWritable(1));
    }
}
