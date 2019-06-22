package com.example.serialize;

import com.alibaba.fastjson.JSON;

import java.io.*;

/**
 * Java的序列化
 * 把对象写入到文件，文件的内容不止对象中的字段，还包括对象的一些其他信息，例如对象的头信息等
 */
public class JavaSerializeMain {


    public static void main(String[] args) throws Exception {
        //将对象信息输出到文件,产生了94B的myFile.txt文件
        MyBean myBean = new MyBean("Lily", 45);

        File file = new File("javaSerializable.txt");
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();
        FileOutputStream fileOutputStream = new FileOutputStream(file);//低层是文件Stream,写入到文件
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);//高层应用接口是对象Stream
        objectOutputStream.writeObject(myBean);
        objectOutputStream.close();

        //从文件读取对象回来
        FileInputStream fileInputStream = new FileInputStream(file);
        ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
        MyBean readMyBean = (MyBean) objectInputStream.readObject();
        System.out.println(JSON.toJSONString(readMyBean));
    }
}
