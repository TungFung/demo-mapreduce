package com.example.serialize;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyBeanWritable implements Writable {

    private String name;

    private int age;

    public MyBeanWritable() {
    }

    public MyBeanWritable(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(age);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.name = in.readUTF();
        this.age = in.readInt();
    }
}
