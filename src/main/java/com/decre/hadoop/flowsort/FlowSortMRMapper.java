package com.decre.hadoop.flowsort;

import com.decre.hadoop.bean.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Decre
 * @date 2019/4/7 0007 20:26
 * @since 1.0.0
 * Descirption: 流量排序map类
 */
public class FlowSortMRMapper  extends Mapper<LongWritable, Text, FlowBean, NullWritable> {

    /**
     * value  = 13602846565    26860680    40332600    67193280
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split("\t");

        FlowBean fb = new FlowBean(split[0], Long.parseLong(split[1]), Long.parseLong(split[2]));

        context.write(fb, NullWritable.get());
    }
}
