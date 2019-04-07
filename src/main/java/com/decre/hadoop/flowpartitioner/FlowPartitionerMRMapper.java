package com.decre.hadoop.flowpartitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Decre
 * @date 2019/4/7 0007 20:45
 * @since 1.0.0
 * Descirption: 省份分区的map类
 */
public class FlowPartitionerMRMapper extends Mapper<LongWritable, Text, Text, Text> {


    /**
     * value  =  13502468823    101663100    1529437140    1631100240
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


        String[] split = value.toString().split("\t");

        String outkey = split[1];
        String outValue = split[8] + "\t" + split[9];

        context.write(new Text(outkey), new Text(outValue));

    }
}
