package com.example.ncdc.max;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxTemperatureReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double maxValue = Integer.MIN_VALUE;
        for (DoubleWritable value : values) {
            maxValue = Math.max(maxValue, value.get());
        }
        context.write(key, new DoubleWritable(maxValue));
    }
}
