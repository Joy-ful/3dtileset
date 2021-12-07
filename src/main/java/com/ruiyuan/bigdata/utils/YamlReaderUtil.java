package com.ruiyuan.bigdata.utils;

import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

public class YamlReaderUtil {
    private static InputStream inputStream;
    //解析yml配置文件为map
    public HashMap<String, Object> yamRead() {
        Yaml yaml = new Yaml();
        inputStream = this.getClass()
                .getClassLoader()
                .getResourceAsStream("config.yml");
        HashMap<String, Object> obj = yaml.load(inputStream);
        //System.out.println(obj);
        return obj;
    }


    public static void closeYaml(){
        try {
            inputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
