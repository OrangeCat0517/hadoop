package com.example.mr;

import java.util.StringTokenizer;

public class Test {
    public static void main(String[] args) {
        String str = "aaa bbb aaa bbb aaa ccc";
        StringTokenizer itr = new StringTokenizer(str);
        while (itr.hasMoreTokens()) {
            System.out.println(itr.nextToken());
        }
    }
}
