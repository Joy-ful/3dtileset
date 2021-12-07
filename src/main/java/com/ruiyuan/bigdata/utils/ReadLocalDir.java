package com.ruiyuan.bigdata.utils;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHost;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ReadLocalDir {

    static YamlReaderUtil yamlReaderUtil = new YamlReaderUtil();
    static HashMap hashMap = yamlReaderUtil.yamRead();
    static String writeLocalFileDir = (String) hashMap.get("writeLocalFilePath");
    static String host = (String) hashMap.get("EsHosts");

    //递归获取文件夹下所有文件 list：所有文件 testFileDir：输入目录
    public void readFileName(String inputFileDir, List<String> list) {

        File[] files = new File(inputFileDir).listFiles();
        for (File file : files) {
            if (file.isFile() && file.getPath().contains("json")) {
                list.add(String.valueOf(file));
            } else if (file.isDirectory()) {
                readFileName(file.getPath(), list);
            }
        }
    }

    //读取一个json地址返回json字符串
    public String readPath2Str(String path) throws IOException {

        InputStream in = new FileInputStream(new File(path));
        String jsonStr = IOUtils.toString(in);
        return jsonStr;
    }

    //文件夹下所有文件夹路径
    public ArrayList<String> readDir(String path) {

        File[] allFileDir = new File(path).listFiles();
        ArrayList<String> dirPath = new ArrayList<>();
        for (File file : allFileDir) {
            if (file.isDirectory()) {
                dirPath.add(file.getPath());
            }
        }
        return dirPath;
    }

    //数据写到本地
    public static void saveAsFileWriter(String tilesName, String content) {

        FileWriter fwriter = null;
        try {
            // true表示不覆盖原来的内容，而是加到文件的后面。若要覆盖原来的内容，直接省略这个参数就好
            fwriter = new FileWriter(writeLocalFileDir, true);
            fwriter.write(tilesName + ":" + content + "\r\n");
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            try {
                fwriter.flush();
                fwriter.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    //遍历获取配置文件中的es ip：port 拼接成新的http请求反回
    public static HttpHost[] httpHost() {

        //解析hostlist配置信息
        String[] split = host.split(",");
        //创建HttpHost数组，其中存放es主机和端口的配置信息
        HttpHost[] httpHostArray = new HttpHost[split.length];
        for (int i = 0; i < split.length; i++) {
            String item = split[i];
            httpHostArray[i] = new HttpHost(item.split(":")[0], Integer.parseInt(item.split(":")[1]), "http");
        }
        return httpHostArray;
    }

}
