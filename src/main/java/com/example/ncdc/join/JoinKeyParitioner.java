package com.example.ncdc.join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class JoinKeyParitioner extends Partitioner<JoinKey, Text> {

    /**
     * JoinKey是区分firstKey和secondKey的
     * 只要firstKey相同，都要放到同一个分区
     * secondKey是用来排序和区分辨别这是一条station记录还是data记录
     */
    @Override
    public int getPartition(JoinKey joinKey, Text value, int numPartitions) {
        return (joinKey.getFirstKey().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
