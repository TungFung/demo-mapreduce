package com.example.count.word;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordCountPartitioner extends Partitioner<Text, IntWritable> {

    @Override
    public int getPartition(Text text, IntWritable intWritable, int i) {
        if(text.toString().startsWith("a")){
            return 0;
        }
        return 1;//此处默认2个reducer为了做实验
    }
}
