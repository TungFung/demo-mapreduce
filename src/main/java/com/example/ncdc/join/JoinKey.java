package com.example.ncdc.join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class JoinKey implements WritableComparable<JoinKey> {

    private Text firstKey;

    private Text secondKey;

    public JoinKey(String firstKey, String secondKey) {
        this.firstKey = new Text(firstKey);
        this.secondKey = new Text(secondKey);
    }

    public JoinKey() {
        this.firstKey = new Text();
        this.secondKey = new Text();
    }

    public JoinKey(Text firstKey, Text secondKey) {
        this.firstKey = firstKey;
        this.secondKey = secondKey;
    }

    public void setFirstKey(Text firstKey) {
        this.firstKey = firstKey;
    }

    public void setSecondKey(Text secondKey) {
        this.secondKey = secondKey;
    }

    public Text getFirstKey() {
        return firstKey;
    }

    public Text getSecondKey() {
        return secondKey;
    }

    @Override
    public String toString() {
        return this.firstKey + "\t" + this.secondKey;
    }

    @Override
    public int hashCode() {
        return this.firstKey.hashCode() * 163 + this.secondKey.hashCode();//分区的时候要用hashCode
    }

    @Override
    public int compareTo(JoinKey o) {
        int compareResult = this.firstKey.compareTo(o.firstKey);//Text中实现了compareTo方法
        if(compareResult != 0){
            return compareResult;
        }
        return this.secondKey.compareTo(o.secondKey);//firstKey相同的情况下比较secondKey
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.firstKey.write(out);//Text中实现了write方法
        this.secondKey.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.firstKey.readFields(in);//Text中实现了readFields方法
        this.secondKey.readFields(in);
    }
}
