package com.decre.hadoop.flowpartitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Decre
 * @date 2019/4/7 0007 20:56
 * @since 1.0.0
 * Descirption: 省份分区reduce类
 */
public class FlowPartitionerMRReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        int upFlow = 0;
        int downFlow = 0;
        int sumFlow = 0;

        for(Text t : values){
            String[] split = t.toString().split("\t");

            int upTempFlow = Integer.parseInt(split[0]);
            int downTempFlow = Integer.parseInt(split[1]);

            upFlow+=upTempFlow;
            downFlow +=  downTempFlow;
        }

        sumFlow = upFlow + downFlow;

        context.write(key, new Text(upFlow + "\t" + downFlow + "\t" + sumFlow));
    }
}
