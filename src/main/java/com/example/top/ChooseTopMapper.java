package com.example.top;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.TreeMap;

public class ChooseTopMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private TreeMap<Integer, String> treeMap = new TreeMap();

    private IntWritable key = new IntWritable();

    private Text value = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lineContents = value.toString().split(" ");
        String cityName = lineContents[0];
        String num = lineContents[1];
        treeMap.put(Integer.valueOf(num), cityName);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        while(treeMap.size() > 2){
            treeMap.remove(treeMap.firstKey());
        }

        for(Integer numKey: treeMap.keySet()){
            String cityValue = treeMap.get(numKey);
            key.set(numKey);
            value.set(cityValue);
            context.write(key, value);
        }
    }
}
