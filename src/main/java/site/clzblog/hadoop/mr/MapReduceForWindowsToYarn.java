package site.clzblog.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import site.clzblog.hadoop.mr.mapper.WordCountMapper;
import site.clzblog.hadoop.mr.reducer.WordCountReducer;

import java.net.URI;

public class MapReduceForWindowsToYarn {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.hostname", "hadoop01");
        //If use windows platform need config this parameter
        conf.set("mapreduce.app-submission.cross-platform", "true");

        Job job = Job.getInstance(conf);

        job.setJar("C:/Users/Administrator/IdeaProjects/hadoop-client-demo/target/hadoop-client-demo-1.0.jar");
        //job.setJarByClass(MapReduceForWindowsToYarn.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));

        Path outputPath = new Path("/wordcount/output");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), conf, "root");
        if (fs.exists(outputPath) && fs.delete(outputPath, true)) System.out.println("Deleted already exists dir");
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setNumReduceTasks(2);

        boolean completion = job.waitForCompletion(true);

        System.out.printf("\nJob completion -> %s\n", completion);

        System.exit(completion ? 0 : 1);
    }
}
