package site.clzblog.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class ApplicationHDFS {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "2");
        conf.set("dfs.blocksize", "64m");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000/"), conf, "root");
        fs.copyFromLocalFile(new Path(""), new Path(""));
        fs.close();
    }
}