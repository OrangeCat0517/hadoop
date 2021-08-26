package com.example.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class SortReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    private static IntWritable data = new IntWritable(1);
    @Override
    //Hadoop默认情况下在reduce阶段会对map阶段产生的键进行排序
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
        for(IntWritable i : values) {
            //有几个值就循环几次
            context.write(data, key);
            data = new IntWritable(data.get() + 1);
        }
    }
}
