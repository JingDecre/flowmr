package com.decre.hadoop.flowsort;

import com.decre.hadoop.bean.FlowBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Decre
 * @date 2019/4/7 0007 20:37
 * @since 1.0.0
 * Descirption: 流量排序Reduce类
 */
public class FlowSortMRReducer extends Reducer<FlowBean, NullWritable, FlowBean, NullWritable> {

    @Override
    protected void reduce(FlowBean key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {


        for (NullWritable nvl : values) {
            context.write(key, nvl);
        }

    }
}
