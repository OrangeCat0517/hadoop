package com.example.hdfs.util;

import org.apache.hadoop.io.Writable;

import java.io.*;

public class HadoopSerializationUtil {
    //序列化的过程就是把一个对象变成字节流的过程
    public static byte[] serialize(Writable writable) {
        //Writable是Hadoop中可序列化内容的父接口，也就是说Hadoop中所有可以被序列化的内容都实现了这个接口
        try(ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
            writable.write(dataOutputStream);
            return byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void deserialize(byte[] bytes, Writable writable) {
        try(ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);) {
            writable.readFields(dataInputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
