package com.neil.bigdata;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowDriver {
    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME","root");
        //1、配置连接hadoop集群的参数
        Configuration conf = new Configuration();
        // access local
        // conf.set("fs.defaultFS", "file:///");
        // conf.set("mapreduce.framework.name", "local");

        // access the cluster
        conf.set("mapreduce.framework.name","yarn");
        conf.set("yarn.resourcemanager.hostname","huawei-01");
        conf.set("fs.defaultFS","hdfs://huawei-01:8063");
        conf.set("dfs.client.use.datanode.hostname", "true");

        //2、获取job对象实例
        Job job =Job.getInstance(conf,"FLOWCOUNT");

        //3、驱动
        job.setJarByClass(FlowDriver.class);

        //4、设置mapper和reducer类型
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReduce.class);

        //5、设置k2,v2,k3,v3的泛型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FLowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FLowBean.class);

        //6、设置reduceTask的个数，默认值是1
        job.setNumReduceTasks(2);

        //7、设置job要处理的数据的输入源
       // FileInputFormat.setInputPaths(job,new Path("D:\\IDEA\\MapReducdDemo1\\src\\main\\resources\\HTTP_20130313143750.dat"));

        // FileInputFormat.setInputPaths(job,new Path("D:\\neil\\TechDS\\TechCamp\\BigDataCamp\\week2-HIVE\\HTTP_20130313143750.dat"));
        // Path outPath = new Path("D:\\neil\\TechDS\\TechCamp\\BigDataCamp\\week2-HIVE\\homework\\output");
        // put the input and output on the hdfs
        FileInputFormat.setInputPaths(job,new Path("hdfs://huawei-01:8063/test/input/HTTP_20130313143750.dat"));
        Path outPath = new Path("hdfs://huawei-01:8063/test/output/flow/");

        FileSystem fs = FileSystem.get(conf);
        //判断输出目录是否存在，如果存在，则删除之
        if(fs.exists(outPath)){
            fs.delete(outPath,true);
        }
        FileOutputFormat.setOutputPath(job,outPath);

        //8、提交job
        System.exit(job.waitForCompletion(true)?0:1);
        System.out.println("job is done..................");
    }
}
