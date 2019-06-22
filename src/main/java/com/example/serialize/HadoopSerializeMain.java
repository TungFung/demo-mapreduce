package com.example.serialize;

import com.alibaba.fastjson.JSON;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;

import java.io.*;

/**
 * 采用hadoop的序列化机制,写入到文件的大小会比java原生的serializable小很多,只存字段的内容
 */
public class HadoopSerializeMain {

    public static void main(String[] args)  throws Exception {
        //将对象信息输出到文件,产生了94B的myFile.txt文件
        MyBeanWritable myBeanWritable = new MyBeanWritable("Lily", 45);

        File file = new File("hadoopSerializable.txt");
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();
        FileOutputStream fileOutputStream = new FileOutputStream(file);//低层是文件Stream,写入到文件
        DataOutputStream dataOutputStream = new DataOutputStream(fileOutputStream);//经DataStream转到文件Stream
        myBeanWritable.write(dataOutputStream);//写入到DataOutputStream
        dataOutputStream.close();

        //从文件读取对象回来
        FileInputStream fileInputStream = new FileInputStream(file);
        DataInputStream dataInputStream = new DataInputStream(fileInputStream);
        Writable writable = WritableFactories.newInstance(MyBeanWritable.class);//读取的时候需要弄个对象来接收,要有无参构造函数
        writable.readFields(dataInputStream);//把DataStream读入对象中
        dataInputStream.close();
        System.out.println(JSON.toJSONString(writable));
    }
}
