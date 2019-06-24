package com.example.ncdc.max;

import com.example.avro.NcdcRecord;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxTemperatureMapper extends Mapper<NullWritable, NcdcRecord, Text, DoubleWritable> {

    private static final double MISSING = 9999.9;

    @Override
    protected void map(NullWritable key, NcdcRecord record, Context context) throws IOException, InterruptedException {
        if (record.getMaxTemp() != MISSING) {
            context.write(new Text(record.getYear().toString()), new DoubleWritable(record.getMaxTemp()));
        }
    }
}
