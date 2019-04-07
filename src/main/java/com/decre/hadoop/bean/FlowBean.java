package com.decre.hadoop.bean;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * @author Decre
 * @date 2019/4/7 0007 20:27
 * @since 1.0.0
 * Descirption:
 * 第一，定义好属性
 * 第二，定义好属性的getter 和 setter方法
 * 第三，定义好构造方法（有参，无参）
 * 第四：定义好toString();
 * <p>
 * <p>
 * 详细解释：
 * <p>
 * 如果一个自定义对象要作为key 必须要实现 WritableComparable 接口， 而不能实现 Writable, Comparable
 * <p>
 * 如果一个自定义对象要作为value，那么只需要实现Writable接口即可
 */
public class FlowBean implements WritableComparable<FlowBean> {
    //public class FlowBean implements Comparable<FlowBean>{

    private String phone;
    private long upFlow;
    private long downFlow;
    private long sumFlow;

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public FlowBean(String phone, long upFlow, long downFlow, long sumFlow) {
        super();
        this.phone = phone;
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = sumFlow;
    }

    public FlowBean(String phone, long upFlow, long downFlow) {
        super();
        this.phone = phone;
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    public FlowBean() {
        super();
        // TODO Auto-generated constructor stub
    }

    @Override
    public String toString() {
        return phone + "\t" + upFlow + "\t" + downFlow + "\t" + sumFlow;
    }


    /**
     * 把当前这个对象 --- 谁掉用这个write方法，谁就是当前对象
     * <p>
     * FlowBean bean = new FlowBean();
     * <p>
     * bean.write(out)    把bean这个对象的四个属性序列化出去
     * <p>
     * this = bean
     */
    public void write(DataOutput out) throws IOException {
        // TODO Auto-generated method stub

        out.writeUTF(phone);
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);

    }


    //   序列化方法中的写出的字段顺序， 一定一定一定要和 反序列化中的 接收顺序一致。 类型也一定要一致


    /**
     * bean.readField();
     * <p>
     * upFlow =
     */
    public void readFields(DataInput in) throws IOException {
        // TODO Auto-generated method stub

        phone = in.readUTF();
        upFlow = in.readLong();
        downFlow = in.readLong();
        sumFlow = in.readLong();

    }


    /**
     * Hadoop的序列化机制为什么不用   java自带的实现 Serializable这种方式？
     *
     * 本身Hadoop就是用来解决大数据问题的。
     *
     * 那么实现Serializable接口这种方式，在进行序列化的时候。除了会序列化属性值之外，还会携带很多跟当前这个对象的类相关的各种信息
     *
     * Hadoop采取了一种全新的序列化机制；只需要序列化 每个对象的属性值即可。
     */


     /*@Override
       public void readFields(DataInput in) throws IOException {
         value = in.readLong();
       }

       @Override
       public void write(DataOutput out) throws IOException {
         out.writeLong(value);
       }*/

    /**
     * 用来指定排序规则
     */
    public int compareTo(FlowBean fb) {
        long diff = this.getSumFlow() - fb.getSumFlow();

        if (diff == 0) {
            return 0;
        } else {
            return diff > 0 ? -1 : 1;
        }

    }
}
