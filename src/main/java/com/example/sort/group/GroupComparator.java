package com.example.sort.group;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

public class GroupComparator implements RawComparator<IntPair> {
    //Raw要求以byte为单位比较（比较两个二进制对象）
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        //比较两个字节数组b1和b2
        //从b1和b2的s1和s2位置处开始比较
        //比多长？由l1和l2来决定。此处的结果是4
        return WritableComparator.compareBytes(b1, s1, Integer.SIZE/8, b2, s2, Integer.SIZE/8);
    }

    //比较两个IntPair：注意这里就是按照first进行比较的
    @Override
    public int compare(IntPair o1, IntPair o2) {
        return o1.getFirst() - o2.getFirst();
    }
}
