package com.example.ncdc.join;

import com.example.avro.NcdcRecord;
import com.example.util.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;

/**
 * 程序目标：把NCDC气象数据中的station和data根据stationId进行关联，输出关联后的文件，以便做进一步的分析使用
 * NCDC的气象站数据是CSV格式的, NCDC的气象指标数据是一个个的gz压缩文件
 *
 * args[0] : 输入目录, station CSV文件 - D:\dfs\input\join\station
 * args[1] : 输入目录，data GZ压缩文件 - D:\dfs\input\join\data
 * args[2] : 输出目录, parquet文件 - D:\dfs\output\join
 */
public class JoinMain {

    public static void main(String[] args) throws Exception {
        Utils.deleteFileIfExists(args[2]);//删除上一次执行的输出结果，否则重复执行会报错

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "data join");//下面的信息其实都是在配置job中的configuration

        job.setJarByClass(JoinMain.class);

        //InputFormat, Mapper, 不同格式的文件，不同的mapper处理, 同时也是不同的格式输入
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, StationMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, DataMapper.class);

        //Partitioner
        job.setPartitionerClass(JoinKeyParitioner.class);

        //GroupingComparator
        job.setGroupingComparatorClass(JoinKeyComparator.class);

        //Reducer , OutputFormat
        job.setReducerClass(JoinReducer.class);
        job.setOutputFormatClass(AvroParquetOutputFormat.class);//使用avro转parquet的格式输出
        AvroParquetOutputFormat.setSchema(job, NcdcRecord.SCHEMA$);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        //声明输出的类型格式
        job.setMapOutputKeyClass(JoinKey.class);//map的输出类型
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);//reduce的输出类型
        job.setOutputValueClass(NcdcRecord.class);

        //提交作业等待执行完毕
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
