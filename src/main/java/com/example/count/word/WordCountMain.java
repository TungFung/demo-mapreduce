package com.example.count.word;

import com.example.util.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountMain {

    public static void main(String[] args) throws Exception {
        String outputFilePath = args[1];
        Utils.deleteFileIfExists(outputFilePath);

        Configuration configuration = new Configuration();
        // 设置MRAppMaster需要的总内存大小为600MB
        configuration.set("yarn.app.mapreduce.am.resource.mb", "600");
        // 设置MRAppMaster需要的堆内存大小为320MB
        configuration.set("yarn.app.mapreduce.am.command-opts", "-Xmx320m");
        // 设置MRAppMaster需要的CPU核心数为1
        configuration.set("yarn.app.mapreduce.am.resource.cpu-vcores", "1");
        // 修改Map Task需要的内存大小为600MB
        configuration.set("mapreduce.map.memory.mb", "600");
        configuration.set("mapreduce.map.java.opts", "-Xmx320m");
        configuration.set("mapreduce.map.cpu.vcores", "1");
        // 修改Reduce Task需要的内存大小为600MB
        configuration.set("mapreduce.reduce.memory.mb", "600");
        configuration.set("mapreduce.reduce.java.opts", "-Xmx320m");
        configuration.set("mapreduce.reduce.cpu.vcores", "1");
        
        Job job = Job.getInstance(configuration, "word count");
        job.setJarByClass(WordCountMain.class);
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(2);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(outputFilePath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
