package com.example.map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MapFileWriter {
    //Map文件是排序的顺序文件
    public static void main(String[] args) throws URISyntaxException, IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), configuration);


        MapFile.Writer writer = new MapFile.Writer(
                configuration, fileSystem,
                new Path("data").toString(),
                IntWritable.class, Text.class);

        var key = new IntWritable(15);
        var value = new Text("hello, world");
        writer.append(key, value);

        key = new IntWritable(100);
        value = new Text("hello, world");
        writer.append(key, value);
        IOUtils.closeStream(writer);
    }
}
