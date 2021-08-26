package com.example.sequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class SequenceFileReader {
    public static void main(String[] args) throws URISyntaxException, IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), configuration);

        SequenceFile.Reader reader =
                new SequenceFile.Reader(fileSystem, new Path("data.seq"), configuration);

        Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), configuration);
        Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), configuration);
        //上面的key和value只是通过反射生成的对象
        if (reader.next(key))
            //reader.next(key)判断有没有下一个key，有的话把这个key放入key中
            reader.getCurrentValue(value);
        System.out.printf("<%s, %s>\n", key, value);

        IOUtils.closeStream(reader);
    }
}
