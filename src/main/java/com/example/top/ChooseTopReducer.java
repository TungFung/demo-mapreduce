package com.example.top;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;

public class ChooseTopReducer extends Reducer<IntWritable, Text, Text, IntWritable> {

    private TreeMap<Integer, String> treeMap = new TreeMap();

    private Text key = new Text();

    private IntWritable value = new IntWritable();

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        treeMap.put(key.get(), values.iterator().next().toString());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        while(treeMap.size() > 2){
            treeMap.remove(treeMap.firstKey());
        }

        for(Integer numKey: treeMap.keySet()){
            String cityValue = treeMap.get(numKey);
            key.set(cityValue);
            value.set(numKey);
            context.write(key, value);
        }
    }
}
