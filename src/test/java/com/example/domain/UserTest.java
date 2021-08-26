package com.example.domain;

import com.example.hdfs.util.HadoopSerializationUtil;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class UserTest {

    @Test
    void test() {
        User tom = new User(1, "Tom", LocalDate.now());
        byte[] bytes = HadoopSerializationUtil.serialize(tom);
        System.out.println(Arrays.toString(bytes));

        User user = new User();
        HadoopSerializationUtil.deserialize(bytes, user);
        System.out.println(user);
    }

}