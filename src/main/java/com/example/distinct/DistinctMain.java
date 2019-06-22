package com.example.distinct;

import com.example.util.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DistinctMain {

    public static void main(String[] args) throws Exception {
        String outputFilePath = args[1];
        Utils.deleteFileIfExists(outputFilePath);

        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration, "distinct");
        job.setJarByClass(DistinctMain.class);
        job.setMapperClass(DistinctMapper.class);
        job.setReducerClass(DistinctReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(outputFilePath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
