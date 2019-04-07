package com.decre.hadoop.flowsort;

import com.decre.hadoop.bean.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * @author Decre
 * @date 2019/4/7 0007 20:40
 * @since 1.0.0
 * Descirption: 流量排序入口类
 */
public class FlowSortMR {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        // 集群上，通过命令行获得输入和输出路径，实现动态化的conf配置
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        //本地运行
        //String[] otherArgs = new String[]{"input/dream.txt","output"};
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordconut <in> [《in>...]<out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "flow sort");

        job.setJarByClass(FlowSortMR.class);

        job.setMapperClass(FlowSortMRMapper.class);
        job.setReducerClass(FlowSortMRReducer.class);

        job.setOutputKeyClass(FlowBean.class);
        job.setOutputValueClass(NullWritable.class);
        // 设置输入输出路径
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        Path outputPath = new Path(otherArgs[otherArgs.length - 1]);

        // 是否安全退出
        boolean isDone = job.waitForCompletion(true);
        System.exit(isDone ? 0 : 1);
    }
}
