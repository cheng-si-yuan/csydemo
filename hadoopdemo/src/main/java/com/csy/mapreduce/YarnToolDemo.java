package com.csy.mapreduce;

import com.csy.tools.HDFSTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;
import java.util.Arrays;

/**
 * 使用yarn tool 接口运行MR的案例
 */
public class YarnToolDemo {

    public static class WordCount implements Tool {
        private Configuration conf;

        @Override
        public int run(String[] args) throws Exception {
            Job job = Job.getInstance(conf);
            job.setJarByClass(YarnToolDemo.class);
            job.setMapperClass(MapReduceDemo.WordCountMapper.class);
            job.setReducerClass(MapReduceDemo.WordCountReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), conf, "csy");
            HDFSTool.deletePathIfExists(fs, args[1]);
            FileInputFormat.setInputPaths(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            fs.close();
            return job.waitForCompletion(true) ? 0 : 1;
        }

        @Override
        public void setConf(Configuration configuration) {
            this.conf = configuration;
        }

        @Override
        public Configuration getConf() {
            return conf;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //  判断是否有 tool 接口
        Tool tool;
        if ("wordcount".equals(args[0])) {
            tool = new WordCount();
        } else {
            throw new RuntimeException(" No such tool: " + args[0]);
        }
        // 用 Tool 执行程序
        // Arrays.copyOfRange 将老数组的元素放到新数组里面
        int run = ToolRunner.run(conf, tool, Arrays.copyOfRange(args, 1, args.length));
        System.exit(run);
    }
}