package com.example.sort.group;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class GroupMapper extends Mapper<LongWritable, Text, IntPair, IntWritable> {
    private final IntPair key = new IntPair();
    private final IntWritable value = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntPair, IntWritable>.Context context) throws IOException, InterruptedException {
        StringTokenizer stringTokenizer =
                new StringTokenizer(value.toString());
        int left;
        int right = 0;
        if(stringTokenizer.hasMoreTokens()){
            left = Integer.parseInt(stringTokenizer.nextToken());
            if(stringTokenizer.hasMoreTokens()){
                right=Integer.parseInt(stringTokenizer.nextToken());
            }
            this.key.setFirst(left);
            this.key.setSecond(right);
            this.value.set(right);
            context.write(this.key, this.value);
        }
        System.out.printf("<%s, %s>", this.key, this.value);
    }
}
