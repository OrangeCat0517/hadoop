package com.example.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class EmpJoinDept {
    public static class EmpDeptMapper extends Mapper<LongWritable, Text, LongWritable, EmpDept> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] arr = value.toString().split("\\s+");
            System.out.println(Arrays.toString(arr));
            EmpDept empDept = new EmpDept();
            if (arr.length <= 3) {
                empDept.setDeptNo(arr[0]);
                empDept.setDeptName(arr[1]);
                empDept.setFlag(1);
                System.err.println("dept:" + new LongWritable(Long.parseLong(arr[0])) + ":" + empDept);
                context.write(new LongWritable(Long.parseLong(arr[0])), empDept);
            } else {
                System.err.println(Arrays.toString(arr));
                empDept.setEmpNo(arr[0]);
                empDept.setEmpName(arr[1]);
                empDept.setDeptNo(arr[6]); //第6列是deptNo
                empDept.setFlag(0);
                System.err.println("xxxxx");
                System.err.println("emp:" +  empDept);
                context.write(new LongWritable(Long.parseLong(empDept.getDeptNo())), empDept);
            }
        }
    }
    //数据混洗
    public static class EmpDeptReduce extends Reducer<LongWritable, EmpDept, NullWritable, Text> {
        @Override
        protected void reduce(LongWritable key, Iterable<EmpDept> values, Context context) throws IOException, InterruptedException {
            EmpDept dept = null; //我用这个变量来表示一个单独的部门
            List<EmpDept> lists = new ArrayList<>();
            for (EmpDept ed : values)
                if (ed.getFlag() == 0) {
                    EmpDept emp = new EmpDept(ed);
                    lists.add(emp);
                } else
                    dept = new EmpDept(ed);


            if (dept != null) {
                for (EmpDept empDept : lists) {
                    empDept.setDeptName(dept.getDeptName());
                    context.write(NullWritable.get(), new Text(empDept.toString()));
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "join");

        job.setJarByClass(EmpJoinDept.class);
        job.setMapperClass(EmpDeptMapper.class);
        job.setReducerClass(EmpDeptReduce.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(EmpDept.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("output"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
