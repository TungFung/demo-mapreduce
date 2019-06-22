package com.example.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

    private static Logger log = LoggerFactory.getLogger(Utils.class);

    public static void deleteFileIfExists(String filePath) {
        Path path = new Path(filePath);
        try{
            FileSystem fileSystem = FileSystem.get(new Configuration());
            if(fileSystem.exists(path)){
                fileSystem.delete(path, true);
            }
        }catch (Exception e){
            log.warn("【删除文件异常】", e);
        }
    }
}
