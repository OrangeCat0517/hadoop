package com.example.sequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.BZip2Codec;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class SequenceFileWriter {
    //SequenceFile：顺序文件，是Hadoop自带的一种用来存放数据的文件格式
    //顺序文件必须有键和值
    //为什么用顺序文件？而不是把普通的文本文件放入hadoop？
    //所以我们希望所有被hadoop处理的数据都带有良好的格式
    //顺序文件可以动态生成，比如从kafka，es，mysql，hive等将数据导出为顺序文件
    public static void main(String[] args) throws URISyntaxException, IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), configuration);

//        SequenceFile.Writer writer = SequenceFile.createWriter(
//                fileSystem, configuration, new Path("data.seq"),
//                IntWritable.class, Text.class);

        SequenceFile.Writer writer = SequenceFile.createWriter(
                fileSystem, configuration, new Path("data.seq"),
                IntWritable.class, Text.class,
                SequenceFile.CompressionType.RECORD, new BZip2Codec());

        for (int i = 0; i < 10000; i++) {
            var key = new IntWritable(i);
            var value = new Text("hello, world");
            writer.append(key, value);
        }


        IOUtils.closeStream(writer);
    }
}
