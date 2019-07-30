package site.clzblog.hadoop.mr.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<IntWritable> iterator = values.iterator();
        int count = 0;
        while (iterator.hasNext()) {
            IntWritable next = iterator.next();
            count += next.get();
        }
        context.write(key, new IntWritable(count));
    }
}
