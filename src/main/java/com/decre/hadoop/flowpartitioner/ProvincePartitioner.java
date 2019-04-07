package com.decre.hadoop.flowpartitioner;

import com.decre.hadoop.bean.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * @author Decre
 * @date 2019/4/7 0007 21:01
 * @since 1.0.0
 * Descirption:
 */
public class ProvincePartitioner extends Partitioner<Text, Text> {


    public static HashMap<String, Integer> proviceDict = new HashMap<String, Integer>();

    static {
        proviceDict.put("136", 0);
        proviceDict.put("137", 1);
        proviceDict.put("138", 2);
        proviceDict.put("139", 3);
    }

    public int getPartition(Text key, Text value, int numPartitions) {
        String prefix = key.toString().substring(0, 3);
        Integer provinceId = proviceDict.get(prefix);
        return provinceId == null ? 4 : provinceId;
    }
}
