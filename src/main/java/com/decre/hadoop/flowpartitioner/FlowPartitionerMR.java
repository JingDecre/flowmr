package com.decre.hadoop.flowpartitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * @author Decre
 * @date 2019/4/7 0007 20:57
 * @since 1.0.0
 * Descirption:
 */
public class FlowPartitionerMR {

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

        Job job = Job.getInstance(conf, "flow partition");

        job.setJarByClass(FlowPartitionerMR.class);

        job.setMapperClass(FlowPartitionerMRMapper.class);
        job.setReducerClass(FlowPartitionerMRReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        /**
         * 非常重要的两句代码
         */
        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(10);

        // 设置输入输出路径
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        Path outputPath = new Path(otherArgs[otherArgs.length - 1]);
        /*FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }*/

        // 是否安全退出
        boolean isDone = job.waitForCompletion(true);
        System.exit(isDone ? 0 : 1);
    }
}
