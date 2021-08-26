package com.example.marge;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class MargeFileRecordReader extends RecordReader<NullWritable, BytesWritable> {
    private FileSplit fileSplit;//文件的一部分
    private Configuration configuration;
    private BytesWritable bytesWritable = new BytesWritable();//Hadoop中的字节数组类型
    private boolean processed = false;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        this.fileSplit = (FileSplit) split;
        this.configuration = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        //将输入路径中的每一个小文件，都读入到一个bytesWritable的里面
        if (processed) {
            byte[] contents = new byte[(int)fileSplit.getLength()];
            Path file = fileSplit.getPath();
            FileSystem fileSystem = file.getFileSystem(configuration);
            FSDataInputStream fsDataInputStream = fileSystem.open(file);

            IOUtils.readFully(fsDataInputStream, contents,0, contents.length);
            //把一个小文件的内容全部读入一个字节数组

            bytesWritable.set(contents, 0, contents.length);
            //将上一步的字节数组放入bytesWritable

            IOUtils.closeStream(fsDataInputStream);
            return true;
        } else
            return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return bytesWritable;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return processed ? 1f : 0f;
    }

    @Override
    public void close() throws IOException {
        //do noting
    }
}
