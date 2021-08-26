package com.example.recordreader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.StringTokenizer;

public class MyInputFormat extends FileInputFormat<IntWritable, Text> {
    @Override
    public RecordReader<IntWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        class MyRecordReader extends RecordReader<IntWritable, Text> {
            private long start; //所读取文件的起始位置
            private long end;   //所读取文件的结束位置
            private FSDataInputStream fsDataInputStream; //在读取的文件上打开一个输入流
            private IntWritable key; //要生成结果的键
            private Text value;//要生成结果的值
            private final Queue<Text> words = new LinkedList<>();

            //将读到的文件内容拆分成单词，把每一个单词放入队列
            @Override
            public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
                FileSplit fileSplit = (FileSplit) split;
                //FileSplit表示Hadoop输入的文件
                start = fileSplit.getStart();//得到一个int值，是这个文件中的当前的位置
                end = start + fileSplit.getLength();
                Configuration configuration = context.getConfiguration();
                Path path = fileSplit.getPath();
                FileSystem fileSystem = path.getFileSystem(configuration);
                fsDataInputStream = fileSystem.open(path);
                fsDataInputStream.seek(start);
                LineReader lineReader = new LineReader(fsDataInputStream);

                StringTokenizer itr;
                Text line = new Text();
                while (lineReader.readLine(line) != 0) {
                    itr = new StringTokenizer(line.toString());
                    while (itr.hasMoreTokens()) {
                        words.offer(new Text(itr.nextToken()));
                    }
                }
                System.out.println(words);
            }

            @Override
            //有没有下一个key，有就返回true并给这个key赋值
            public boolean nextKeyValue() throws IOException, InterruptedException {
                if (key == null)
                    key = new IntWritable(0);
                if (value == null)
                    value = new Text();

                if (words.isEmpty())
                    return false;
                else {
                    key.set(key.get() + 1);
                    value.set(words.poll());
                    return true;
                }
            }

            @Override
            public IntWritable getCurrentKey() throws IOException, InterruptedException {
                return key;
            }

            @Override
            public Text getCurrentValue() throws IOException, InterruptedException {
                return value;
            }

            @Override
            public float getProgress() throws IOException, InterruptedException {
                return 0;
            }

            @Override
            public void close() throws IOException {
                fsDataInputStream.close();
            }
        }
        return new MyRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
}
