package com.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DistributedCountMapper extends Mapper<Object, Text, Text, IntWritable> {

    private static final Text KEY = new Text("count");

    private static final IntWritable VALUE = new IntWritable(1);

    /**
     * 文件中每读到一行，就会调用一次map函数，输出一对key,value
     * 全部map-reduce程序都跑完后，会收集起来，根据key进行归并
     * 像这里每次都输出 count,1 ,假设文件有3行那么会输出一个 count, [1,1,1] 给到 reduce函数进行进一步处理
     */
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        context.write(KEY, VALUE);
    }
}
