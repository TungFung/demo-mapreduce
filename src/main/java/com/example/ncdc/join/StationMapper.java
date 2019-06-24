package com.example.ncdc.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class StationMapper extends Mapper<LongWritable, Text, JoinKey, Text> {

    /**
     * 将station csv文件中的前两个字段用 “-” 号拼接起来组成ID,输出值，就是整行记录
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String stationId = getStationId(value.toString());

        //如果是表头，则这次map不输出任何内容，相当于跳过
        if(stationId != null){
            context.write(new JoinKey(stationId, "0"), value);
        }
    }

    private String getStationId(String line) {
        String[] values = line.split(",");//输入是CSV格式的，以逗号分隔

        //遇到表头返回空
        if ("USAF".equals(values[0]) || values.length != 11) {
            return null;
        }

        //每个字段是用双引号扣起来的，所以要去掉双引号
        String firstField = values[0].replace("\"", "");
        String secondField = values[1].replace("\"", "");
        StringBuffer sb = new StringBuffer();
        sb.append(firstField);
        sb.append("-");
        sb.append(secondField);
        return sb.toString();
    }
}
