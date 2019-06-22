package com.example.top;

import com.example.util.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ChooseTopMain {

    public static void main(String[] args) throws Exception {
        String outputFilePath = args[1];
        Utils.deleteFileIfExists(outputFilePath);

        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration, "choose top");
        job.setJarByClass(ChooseTopMain.class);
        job.setMapperClass(ChooseTopMapper.class);
        job.setReducerClass(ChooseTopReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(outputFilePath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
