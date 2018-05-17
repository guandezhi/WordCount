package com.bigdata.mr.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 单词统计的Mapper类，继承Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 * 输入的key是map的输出key，是单词，所以是String类型
 * 输入的value是map的输出value，是单词的数量，所以是int类型
 * 输出的key是单词，所以是String类型
 * 输出的value是单词出现的次数，所以是int类型
 * @author guandezhi
 * @date 2018/5/17 14:22
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    // 定义变量，接收单词的出现次数
    private IntWritable value = new IntWritable();


    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 定义一个变量，用于统计单词的数量
        Integer num = 0;

        for (IntWritable i : values) {
            num += i.get();
        }
        value.set(num);
        context.write(key, value);
    }
}
