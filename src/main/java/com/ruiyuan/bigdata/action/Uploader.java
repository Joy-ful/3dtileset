package com.ruiyuan.bigdata.action;

import com.alibaba.fastjson.JSON;
import com.ruiyuan.bigdata.bean.TilesetObj;
import com.ruiyuan.bigdata.bean.nodes.RootNode;
import com.ruiyuan.bigdata.utils.*;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;

/**
 * Uploader
 */

public class Uploader {

    private static Logger logger = LogManager.getLogger(Uploader.class.getName());

    private static EsUtil esUtil = new EsUtil();

    public void Upload(String localTilesPath, String hdfsTilesPath) throws Exception {

        /**
         * 1、上传所有b3dm文件到hdfs
         * 2、（运行瓶颈！！）构建b3dm本地绝对路径与hdfs sequencefile url之间的对应关系
         * 3、递归遍历localTilesPath下所有文件（json和b3dm）,获取所有文件本地路径列表
         * 4、将json中的所有本地路径修改为url
         *  对于每一个json文件
         *  1）反序列化为TilesetObj
         *  2）（重要！！）修改所有uri字段，将json文件地址对应到ES url，b3dm文件地址对应到HDFS sequence file url
         *  3）修改后的TilesetObj序列化为json
         */

        ReadLocalDir readLocalDir = new ReadLocalDir();

        //1
        logger.info("Upload b3dm to HDFS");
        WriteB3dm writeB3dm = new WriteB3dm();
        writeB3dm.writeB3dm2HDFS(localTilesPath, hdfsTilesPath);

        //2
        logger.info("get b3dm url from hdfs");
        ReadB3dm readB3dm = new ReadB3dm();
        HashMap<String, String> b3dmLocalpath_Url = readB3dm.readHDFSSequenceFile(hdfsTilesPath, localTilesPath, hdfsTilesPath);

        //3
        logger.info("遍历tileset下json文件");
        List<String> localJsonPaths = new ArrayList<>();
        readLocalDir.readFileName(localTilesPath, localJsonPaths);

        //4
        logger.info("开始修改json里的uri");
        TilesetObj tilesetObj = new TilesetObj();

        for (String localJsonPath : localJsonPaths) {

            //1）
            String jsonStr = IOUtils.toString(new FileInputStream(localJsonPath));
            TilesetObj tilesetJson = tilesetObj.getTilesetObj(jsonStr);

            //2）
            tilesetJson.setB3dmHdfsUri(b3dmLocalpath_Url, localJsonPath);
            RootNode root = tilesetJson.getRoot();
            tilesetObj.setPntsUri(root, b3dmLocalpath_Url, localJsonPath, localTilesPath);

            //3）
            String tilesetJSON = JSON.toJSONString(tilesetJson);

            //json文件地址\最后字段
            String idLast = localJsonPath
                    .substring(localJsonPath.lastIndexOf("\\", localJsonPath.lastIndexOf("\\") - 1) + 1)
                    .replace("\\", "_");

            //取localTilesPath文件\分割后的最后字段
            String substring = localTilesPath.substring(localTilesPath.lastIndexOf("\\") + 1);

            //拼接es的id
            String esId = substring + "_" + idLast;

            //上传es
            esUtil.UpJson2ES(tilesetJSON, esId);

        }

        logger.info("json里的uri修改结束");

    }
}
