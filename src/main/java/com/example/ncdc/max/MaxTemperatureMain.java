package com.example.ncdc.max;


import com.example.avro.NcdcRecord;
import com.example.util.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.parquet.avro.AvroParquetInputFormat;

/**
 * 分析全球每年的最大气温
 */
public class MaxTemperatureMain {

    public static void main(String[] args) throws Exception {
        Utils.deleteFileIfExists(args[1]);

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "analyze max temperature");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        AvroParquetInputFormat.setAvroReadSchema(job, NcdcRecord.SCHEMA$);
        job.setInputFormatClass(AvroParquetInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setJarByClass(MaxTemperatureMain.class);
        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
