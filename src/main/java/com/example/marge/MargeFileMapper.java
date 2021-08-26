package com.example.marge;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class MargeFileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
    private Text filenameKey;

    @Override
    protected void setup(Mapper<NullWritable, BytesWritable, Text, BytesWritable>.Context context) throws IOException, InterruptedException {
        InputSplit split = context.getInputSplit();
        Path path = ((FileSplit) split).getPath();
        filenameKey = new Text(path.toString());
    }

    @Override
    protected void map(NullWritable key, BytesWritable value, Mapper<NullWritable, BytesWritable, Text, BytesWritable>.Context context) throws IOException, InterruptedException {
        context.write(filenameKey, value);
    }
}
