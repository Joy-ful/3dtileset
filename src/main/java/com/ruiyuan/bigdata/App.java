package com.ruiyuan.bigdata;

import com.ruiyuan.bigdata.action.UpLoadThread;
import com.ruiyuan.bigdata.utils.EsUtil;
import com.ruiyuan.bigdata.utils.ReadLocalDir;
import com.ruiyuan.bigdata.utils.YamlReaderUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
public class App {

        private static Logger logger = LogManager.getLogger(App.class.getName());

    public static void main(String[] args) {

        logger.info("线程开始");

        ExecutorService executorService = Executors.newFixedThreadPool(10);

        //read configures
        YamlReaderUtil yamlReaderUtil = new YamlReaderUtil();
        HashMap hashMap = yamlReaderUtil.yamRead();
        String inputDir = (String) hashMap.get("inputAllDir");

        //read local 3dtiles data
        ReadLocalDir readLocalDir = new ReadLocalDir();
        ArrayList<String> tilesPaths = readLocalDir.readDir(inputDir);

        log.info(" 输入文件:[{}]", inputDir);
        logger.info("输入文件目录循环");
        log.info(" 各文件路径:[{}]", tilesPaths);

        //启动线程
        try {
            for (String tilesPath : tilesPaths) {
                UpLoadThread upLoadThread = new UpLoadThread();
                upLoadThread.setTilesPath(tilesPath);
                executorService.execute(upLoadThread);
            }

        } finally {
            try {

                executorService.shutdown();

                while (!executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS)) {  //这里表示近似永远等待

                    logger.info("线程进行中");

                }

                logger.info("线程结束");

                //关闭es客户端
                EsUtil.ClientClose();

                //关闭yamlutil解析器
                YamlReaderUtil.closeYaml();

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
