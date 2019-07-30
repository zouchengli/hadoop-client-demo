package site.clzblog.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;

public class ApplicationHDFS {
    public static void main(String[] args) throws Exception {
        //copyFromLocalFile();
        //copyToLocalFile();
        //mkdirs();
        //rename();
        //delete();
        //list();
        //listAll();
        readFile();
        randomReadFile();
    }

    private static Configuration getConf() {
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "2");
        conf.set("dfs.blocksize", "64m");
        return conf;
    }

    public static FileSystem getFileSystem() {
        try {
            return FileSystem.get(new URI("hdfs://192.168.1.16:9000/"), getConf(), "root");
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void copyFromLocalFile() throws Exception {
        FileSystem fs = getFileSystem();
        if (fs == null) return;
        fs.copyFromLocalFile(new Path("C:/Users/Administrator/Downloads/jdk-8u181-linux-x64.tar.gz"), new Path("/"));
        System.out.println("Copy from local file successfully");
        fs.close();
    }

    public static void copyToLocalFile() throws Exception {
        FileSystem fs = getFileSystem();
        if (fs == null) return;
        fs.copyToLocalFile(new Path("/anaconda-ks.cfg"), new Path("C:/Users/Administrator/Downloads/"));
        System.out.println("Copy to local file successfully");
        fs.close();
    }

    public static void rename() throws Exception {
        FileSystem fs = getFileSystem();
        if (fs == null) return;
        fs.rename(new Path("/jdk/jdk-8u181-linux-x64.tar.gz"), new Path("/jdk/1.8/jdk-8u181-linux-x64.tar.gz"));
        System.out.println("Rename successfully");
        fs.close();
    }

    public static void mkdirs() throws Exception {
        FileSystem fs = getFileSystem();
        if (fs == null) return;
        fs.mkdirs(new Path("/jdk/1.8"));
        System.out.println("Mkdirs successfully");
        fs.close();
    }

    public static void delete() throws Exception {
        FileSystem fs = getFileSystem();
        if (fs == null) return;
        fs.delete(new Path("/jdk"), true);
        System.out.println("Delete successfully");
        fs.close();
    }

    public static void list() throws Exception {
        FileSystem fs = getFileSystem();
        if (fs == null) return;
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path("/"), true);
        while (iterator.hasNext()) {
            LocatedFileStatus fileStatus = iterator.next();
            System.out.println("===========================================================");
            System.out.printf("Block size:%s\n", fileStatus.getBlockSize());
            System.out.printf("File size:%s\n", fileStatus.getLen());
            System.out.printf("File path:%s\n", fileStatus.getPath());
            System.out.printf("Block replication:%s\n", fileStatus.getReplication());
            System.out.printf("Block info:%s\n", Arrays.toString(fileStatus.getBlockLocations()));
            System.out.println("===========================================================");
        }
        fs.close();
    }

    public static void listAll() throws Exception {
        FileSystem fs = getFileSystem();
        if (fs == null) return;
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus: listStatus) {
            System.out.println("===========================================================");
            System.out.printf("Block size:%s\n", fileStatus.getBlockSize());
            System.out.printf("File size:%s\n", fileStatus.getLen());
            System.out.printf("File path:%s\n", fileStatus.getPath());
            System.out.printf("File type:%s\n", fileStatus.isFile() ? "is file" : "is dir");
            System.out.printf("Block replication:%s\n", fileStatus.getReplication());
            System.out.println("===========================================================");
        }
        fs.close();
    }

    public static void readFile() throws Exception {
        FileSystem fs = getFileSystem();
        if (fs == null) return;
        FSDataInputStream in = fs.open(new Path("/anaconda-ks.cfg"));
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        String line;
        while ((line = reader.readLine()) != null) System.out.println(line);
        reader.close();
        in.close();
        fs.close();
    }

    public static void randomReadFile() throws Exception {
        FileSystem fs = getFileSystem();
        if (fs == null) return;
        FSDataInputStream in = fs.open(new Path("/anaconda-ks.cfg"));
        in.seek(14);
        byte[] bytes = new byte[35];
        int read = in.read(bytes);
        System.out.println(read);
        System.out.println(new String(bytes));
        in.close();
        fs.close();
    }
}
