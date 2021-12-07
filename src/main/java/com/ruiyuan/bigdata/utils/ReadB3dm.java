package com.ruiyuan.bigdata.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Set;

public class ReadB3dm {

    //read  读取hdfs的seqfile文件返回<b3dm绝对路径，新的uri>
    //从hdfs上读取sequenceFile
    public HashMap<String, String> readHDFSSequenceFile(String path, String inputDri, String outputDri) throws IOException {
        YamlReaderUtil yamlReaderUtil = new YamlReaderUtil();
        HashMap hashMap = yamlReaderUtil.yamRead();
        String HDFS_HOSTS = (String) hashMap.get("HDFS_HOSTS");
        String esHDFS_PATH = (String) hashMap.get("esHDFS_PATH");

        //hdfs的配置
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(HDFS_HOSTS), conf);
        //write b3dm
        WriteB3dm writeB3dm = new WriteB3dm();
        HashMap<Text, Long> read = writeB3dm.writeB3dm2HDFS(inputDri, outputDri);

        Path newpath = new Path(path);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, newpath, conf);
        Text keys = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), conf);

        Set<Text> texts = read.keySet();

        HashMap<String, String> map = new HashMap<>();

        while (reader.next(keys)) {
            for (Text text : texts) {
                //上传文件名与读取的文件名
                if (text.equals(keys)) {
                    String string = text.toString();

                    //文件指针
                    long position = reader.getPosition();
                    //b3dm->length
                    Long aLong = read.get(text);
                    // offset
                    long offset = position - aLong;
                    //新的http         ！！修改处！！
                    //String http = "http://" + esHDFS_PATH + "/webhdfs/v1" + path + "?op=OPEN&offset=";
                    //String http = "http://10.10.13.177:8988/" +  "webhdfs/v1" + path + "?op=OPEN&user.name=root&offset=";
                    //                                                      !!修改处!!
                    String http = "webhdfs/v1" + path + "?op=OPEN&user.name=root&offset=";
                    String length = "&length=";
                    String value = http + offset + length + aLong;
                    map.put(string, value);
                }
            }
        }
        reader.close();
        return map;
    }
}
