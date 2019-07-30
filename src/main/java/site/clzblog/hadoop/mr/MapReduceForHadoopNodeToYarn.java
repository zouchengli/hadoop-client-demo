package site.clzblog.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import site.clzblog.hadoop.mr.mapper.WordCountMapper;
import site.clzblog.hadoop.mr.reducer.WordCountReducer;

public class MapReduceForHadoopNodeToYarn {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(MapReduceForHadoopNodeToYarn.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));

        FileOutputFormat.setOutputPath(job, new Path("/wordcount/output"));

        job.setNumReduceTasks(4);

        boolean completion = job.waitForCompletion(true);

        System.out.printf("\nJob completion -> %s\n", completion);

        System.exit(completion ? 0 : 1);
    }
}
