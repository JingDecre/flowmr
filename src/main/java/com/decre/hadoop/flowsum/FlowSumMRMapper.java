package com.decre.hadoop.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Decre
 * @date 2019/4/7 0007 19:58
 * @since 1.0.0
 * Descirption: 流量统计map类
 */
public class FlowSumMRMapper extends Mapper<LongWritable, Text, Text, Text> {

    protected void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException {

        // 根据制表符进行切分
        String[] split = value.toString().split("\t");

        String outkey = split[1];

        String outValue = split[8] + "\t" + split[9];

        context.write(new Text(outkey), new Text(outValue));
    }
}
