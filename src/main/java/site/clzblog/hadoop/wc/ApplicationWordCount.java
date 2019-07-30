package site.clzblog.hadoop.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import site.clzblog.hadoop.wc.context.Context;
import site.clzblog.hadoop.wc.mapper.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.Properties;

public class ApplicationWordCount {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.load(ApplicationWordCount.class.getClassLoader().getResourceAsStream("wc.properties"));
        Class<?> mapperClass = Class.forName(properties.getProperty("mapper"));
        Mapper mapper = (Mapper) mapperClass.newInstance();

        Context context = new Context();

        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.16:9000"), new Configuration(), "root");
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/wordcount/input/"), false);
        while (files.hasNext()) {
            LocatedFileStatus file = files.next();
            FSDataInputStream in = fs.open(file.getPath());
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line;
            while ((line = reader.readLine()) != null)  mapper.map(line, context);
            reader.close();
            in.close();
        }

        Map<Object, Object> map = context.getMap();
        Path outPath = new Path("/wordcount/output/");
        if (!fs.exists(outPath) && fs.mkdirs(outPath)) System.out.printf("Mkdirs %s successfully", outPath.toString());

        FSDataOutputStream out = fs.create(new Path("/wordcount/output/result.txt"));
        map.forEach((key, value) -> {
            try {
                out.write((String.format("%s\t%s\n", key, value)).getBytes());
                out.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        out.close();
        fs.close();

        System.out.println("Word count job completed!!!");
    }
}
