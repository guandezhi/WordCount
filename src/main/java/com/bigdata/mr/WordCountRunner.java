package com.bigdata.mr;

import com.bigdata.mr.map.WordCountMapper;
import com.bigdata.mr.reducer.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 统计单词数量
 * @author guandezhi
 * @date 2018/5/17 13:59
 */
public class WordCountRunner {

    /**
     * 程序入口
     * @param args
     */
    public static void main(String[] args) {
        try {
            // 定义一个job
            Job job = Job.getInstance(new Configuration(), "wordCountMR");
            // 指定主类
            job.setJarByClass(WordCountRunner.class);
            // 指定map类
            job.setMapperClass(WordCountMapper.class);
            // 指定map的输出key、value类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            // 指定reducer类
            job.setReducerClass(WordCountReducer.class);
            // 指定reducer的输出key、value类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            // 设置要处理的文件的位置
            FileInputFormat.setInputPaths(job, new Path("/hello.txt"));
            // 设置输出文件的位置
            FileOutputFormat.setOutputPath(job, new Path("/hello_complete.txt"));
            // 任务提交
            boolean result = job.waitForCompletion(true);
            System.exit(result ? 0 : -1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
