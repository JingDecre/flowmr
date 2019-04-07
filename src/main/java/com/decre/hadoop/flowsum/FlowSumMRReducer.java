package com.decre.hadoop.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Decre
 * @date 2019/4/7 0007 20:03
 * @since 1.0.0
 * Descirption: 手机流量统计的reduce类
 */
public class FlowSumMRReducer extends Reducer<Text, Text, Text, Text> {

    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        int upFlow = 0;
        int downFlow  = 0;
        int sumFlow = 0;

        for (Text t: values) {
            String[] split = t.toString().split("\t");

            int upTempFlow = Integer.parseInt(split[0]);
            int downTempFlow = Integer.parseInt(split[1]);

            upFlow += upTempFlow;
            downFlow += downTempFlow;
        }

        sumFlow = upFlow + downFlow;

        context.write(key, new Text(upFlow + "\t" + downFlow + "\t" + sumFlow));
    }
}
