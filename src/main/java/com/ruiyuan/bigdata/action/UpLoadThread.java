package com.ruiyuan.bigdata.action;

import com.ruiyuan.bigdata.utils.ReadLocalDir;
import com.ruiyuan.bigdata.utils.YamlReaderUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.HashMap;

/**
 * 线程类，每个线程负责上传一个3dtiles数据
 */

@Slf4j
public class UpLoadThread implements Runnable {

    private static Logger logger = LogManager.getLogger(UpLoadThread.class.getName());

    YamlReaderUtil yamlReaderUtil = new YamlReaderUtil();
    HashMap hashMap = yamlReaderUtil.yamRead();
    String hdfsDir = (String) hashMap.get("hdfsDir");
    String esIpAndPort = (String) hashMap.get("esIpAndPort");
    String esindex = (String) hashMap.get("esindex");
    String estype = (String) hashMap.get("estype");

    private String localTilesPath;

    public void setTilesPath(String tilesPath) {
        this.localTilesPath = tilesPath;
    }

    @Override
    public void run() {

        logger.info("开始构建索引");

        //取文件名
        String tilesName = localTilesPath.split("\\\\")[localTilesPath.split("\\\\").length - 1];
        String hdfsTilesPath = hdfsDir + tilesName;

        log.info("hdfs文件路径·:[{}]", hdfsTilesPath);

        Uploader uploader = new Uploader();

        try {
            uploader.Upload(localTilesPath, hdfsTilesPath);
        } catch (Exception e) {
            e.printStackTrace();
        }

        /*        String http = "http://";
        String x = "/";
        String _source = "_source";
        String _tileset = "_tileset.json";
        String tilesetEsUrl = http + esIpAndPort + x + esindex + x + estype + x + tilesName + "_" + tilesName + _tileset + x + _source;*/

        //http://%s
        String tilesetEsUrl = String.format("%s/%s/%s_%s_tileset.json/_source",
                //esIpAndPort,
                esindex,
                estype,
                tilesName,
                tilesName);

        logger.info("构建索引结束");

        //写到本地文本，格式———— tilesName:https
        ReadLocalDir.saveAsFileWriter(tilesName,tilesetEsUrl);

        System.out.println("tiles索引: " + tilesetEsUrl);
    }
}
