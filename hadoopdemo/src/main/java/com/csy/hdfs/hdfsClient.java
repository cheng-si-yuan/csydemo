package com.csy.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class hdfsClient {
    // 创建目录
    @Test
    public void testMkdir() throws IOException, URISyntaxException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), configuration, "csy");
        fs.mkdirs(new Path("/metal/blackmetal"));
        fs.close();
    }

    // 从本地上传
    @Test
    public void testCopyFromLocalFile() throws IOException, URISyntaxException, InterruptedException {
        Configuration configuration = new Configuration();
//        configuration.set("dfs.replication", "2");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), configuration, "csy");
        fs.copyFromLocalFile(
                new Path("C:/Users/csy/Desktop/资料/10_google三篇论文/Google-File-System中文版_1.0.pdf"),
                new Path("/metal/blackmetal")
        );
        fs.close();
    }

    // 下载到本地
    @Test
    public void testCopyToLocalFile() throws IOException, URISyntaxException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), configuration, "csy");
        fs.copyToLocalFile(false,
                new Path("/metal/blackmetal/Google-File-System中文版_1.0.pdf"),
                new Path("C:/Users/csy/Desktop/"), false
        );
        fs.close();
    }

    // 删除
    @Test
    public void testDelete() throws IOException, URISyntaxException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), configuration, "csy");
        fs.delete(new Path("/metal/blackmetal/Google-File-System中文版_1.0.pdf"), true);
        fs.close();
    }

    // 查看文件列表详情
    @Test
    public void testListFiles() throws IOException, URISyntaxException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), configuration, "csy");
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while(listFiles.hasNext()){
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println("========" + fileStatus.getPath() + "=========");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());
            // 获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
        fs.close();
    }

    // 判断是否为文件
    @Test
    public void testisFile() throws IOException, URISyntaxException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), configuration, "csy");
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : listStatus) {
            // 如果是文件
            if (fileStatus.isFile()) {
                System.out.println("f:"+fileStatus.getPath().getName());
            }else {
                System.out.println("d:"+fileStatus.getPath().getName());
            }
        }
        fs.close();
    }
}