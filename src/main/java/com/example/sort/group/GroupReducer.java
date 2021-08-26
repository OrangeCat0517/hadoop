package com.example.sort.group;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GroupReducer extends Reducer<IntPair, IntWritable, Text, IntWritable> {
    private final Text first = new Text();

    @Override
    protected void reduce(IntPair key, Iterable<IntWritable> values, Reducer<IntPair, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        //到了这个阶段，已经是分组过后的结果。同时，值IntPair也进行了排序
        first.set(Integer.toString(key.getFirst()));
        for (IntWritable i : values)
            context.write(first, i);
    }
}
