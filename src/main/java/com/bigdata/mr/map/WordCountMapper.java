package com.bigdata.mr.map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 单词统计的Mapper类，继承Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 * 输入的key是文本偏移量，所以是long类型
 * 输入的value是单词，所以是String类型
 * 输出的key是单词，所以是String类型
 * 输出的value是单词出现的次数，所以是int类型
 * @author guandezhi
 * @date 2018/5/17 14:12
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    // 定义mapper的输出key value
    private Text k = new Text();
    private IntWritable v = new IntWritable(1); // 出现一次记录1

    /**
     * 实现Mapper的map方法。
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 输入文本内容
        String lines = value.toString();
        // 拆分单词
        String[] words = lines.split(" ");

        for (String word : words) {
            // 将单词设置到输出的key中
            k.set(word);
            // 设置输出值
            context.write(k, v);
        }

    }
}
