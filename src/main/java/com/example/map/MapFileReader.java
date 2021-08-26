package com.example.map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MapFileReader {

    public static void main(String[] args) throws URISyntaxException, IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), configuration);

        MapFile.Reader reader =
                new MapFile.Reader(fileSystem,
                        new Path("data").toString(),
                        configuration);
        Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), configuration);
        Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), configuration);

        if(reader.next((WritableComparable) key, value))
            System.out.printf("<%s, %s>\n", key, value);
        IOUtils.closeStream(reader);
    }
}
