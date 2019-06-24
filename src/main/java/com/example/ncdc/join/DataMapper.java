package com.example.ncdc.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DataMapper extends Mapper<LongWritable, Text, JoinKey, Text> {

    /**
     * 将data记录中的前两个字段用 “-” 号拼接起来组成ID,输出值，就是整行记录
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String stationId = getStationId(value.toString());//解析data的数据，获取其中的前两个字段作为Id

        //如果是表头，则这次map不输出任何内容，相当于跳过
        if(stationId != null){
            context.write(new JoinKey(stationId, "1"), value);
        }
    }

    private String getStationId(String line) {
        if ("STN".equals(line.substring(0, 3))) { //head
            return null;
        }
        return line.substring(0, 6) + "-" + line.substring(7, 12);
    }
}
