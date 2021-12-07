package com.ruiyuan.bigdata.utils;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.*;
import java.net.URI;
import java.util.HashMap;

public class WriteB3dm {


    //write把b3dm文件合并为seqfile 并上传hdfs
    //输入参数  inputDri：本地路径 outputDri：hdfs路径
    public HashMap<Text, Long> writeB3dm2HDFS(String inputDri, String outputDri) throws IOException {
        YamlReaderUtil yamlReaderUtil = new YamlReaderUtil();
        HashMap hashMap = yamlReaderUtil.yamRead();
        String HDFS_PATH = (String) hashMap.get("HDFS_HOSTS");

        //hdfs的配置
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(HDFS_PATH), conf);
        //hdfs存储路径
        Path output = new Path(outputDri);
        //Writer对象
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, output, Text.class, BytesWritable.class, SequenceFile.CompressionType.NONE);
        //本地文件路径
        Path inputDir = new Path(inputDri);

        File inputDirPath = new File(String.valueOf(inputDir));
        HashMap<Text, Long> map = new HashMap();

        //1,2 遍历文件夹，得到所有的b3dm上传hdfs得到map<k:b3dm的绝对路径,v:value的长度>
        if (inputDirPath.isDirectory()) {
            //获取目录中的文件
            File[] files = inputDirPath.listFiles();
            //迭代文件
            for (File file : files) {
                if (file.isDirectory()) {
                    File[] files1 = file.listFiles();
                    for (File file1 : files1) {
                        if (file1.getName().endsWith(".b3dm")) {
                            //获取文件的全部内容

                            //获取文件名
                            String filePath = file1.getPath();
                            //文件名 test类型
                            Text text = new Text(filePath.getBytes());
                            InputStream in = new FileInputStream(new File(filePath));
                            byte[] bytes = IOUtils.toByteArray(in);
                            in.read(bytes);

                            //文件内容
                            BytesWritable value = new BytesWritable(bytes);
                            //文件长度
                            long length = value.getLength();
                            //数据添加
                            map.put(text, length);
                            //向SequenceFile中写入数据
                            writer.append(text, value);
                        }
                    }
                }
            }
        }
        writer.close();
        return map;
    }
}
