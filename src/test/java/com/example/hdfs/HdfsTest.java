package com.example.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


class HdfsTest {

    Configuration configuration;
    FileSystem fileSystem;

    @BeforeEach
    void setUp() throws IOException, URISyntaxException {
        configuration = new Configuration();
        fileSystem = FileSystem.get(
                new URI("hdfs://localhost:9000"), configuration);
    }

    @AfterEach
    void tearDown() {
        fileSystem = null;
        configuration = null;
    }

    @Test
    void mkdir() throws IOException {
        fileSystem.mkdirs(
                new Path("/peter/test"));
    }

    @Test
    void copyFromLocalFile() throws IOException {
        fileSystem.copyFromLocalFile(
                new Path("/Users/peter/test.c"),
                new Path("/peter/test"));
    }
}