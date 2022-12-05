package com.csy.tools;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HDFSTool {

    /**
     * 当输入HDFS目录存在时，递归删除目录
     */
    public static void deletePathIfExists(FileSystem fs, String pathStr) throws IOException {
        Path path = new Path(pathStr);
        if (fs.exists(path)) {
            System.out.println("====== path delete: " + pathStr);
            fs.delete(path, true);
        }
    }
}