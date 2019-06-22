package com.example.count.word;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text word = new Text();

    private IntWritable count = new IntWritable(1);

    /**
     * 文件中每读到一行，就会调用一次map函数，输入key是每行的偏移量，value是每行的内容.输出一对key,count
     * 全部map-reduce程序都跑完后，会收集起来，根据key进行归并
     * 像这里每次都输出 count,1 ,假设文件有3行那么会输出一个 count, [1,1,1] 给到 reduce函数进行进一步处理
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        while(tokenizer.hasMoreElements()){
            word.set(tokenizer.nextToken());
            context.write(this.word, this.count);
        }
    }
}
