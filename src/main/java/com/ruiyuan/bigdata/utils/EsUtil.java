package com.ruiyuan.bigdata.utils;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;

//es索引上传
public class EsUtil {

    private static YamlReaderUtil yamlReaderUtil;
    private static HashMap yamlMap;
    private static int esport;
    private static String eshostname;
    //private static String eshostname2;
    //private static String eshostname3;
    private static String esindex;
    private static String estype;
    private static RestHighLevelClient client;

    static {
        //yul配置文件属性获取
        yamlReaderUtil = new YamlReaderUtil();
        yamlMap = yamlReaderUtil.yamRead();

        //es ip
        eshostname = (String) yamlMap.get("eshostname");

        /*eshostname2 = (String) yamlMap.get("eshostname2");
        eshostname3 = (String) yamlMap.get("eshostname3");*/

        //es port
        esport = (int) yamlMap.get("esport");

        //es index
        esindex = (String) yamlMap.get("esindex");

        //es type
        estype = (String) yamlMap.get("estype");

        //es client
        client = new RestHighLevelClient(RestClient.builder(ReadLocalDir.httpHost()));

        /*new HttpHost(eshostname1, esport, "http"),
                new HttpHost(eshostname2, esport, "http"),
                new HttpHost(eshostname3, esport, "http")*/

    }

    //jsones json:数据   id:es的id
    public void UpJson2ES(String tilesJSON, String esId) throws Exception {

        /*String index ="";
        String type="";
        String esid="";*/

        //批量操作处理
        BulkRequest request1 = new BulkRequest();
        request1.add(new IndexRequest(esindex, estype, esId).source(tilesJSON, XContentType.JSON));
        client.bulk(request1, RequestOptions.DEFAULT);

        /*final BulkItemResponse[] items = bulk.getItems();

        for (BulkItemResponse item : items) {
            esid = item.getId();
            index = item.getIndex();
            type = item.getType();
        }

       单次操作来一条插入一条
        // 唯一编号
        IndexRequest request = new IndexRequest(esindex, estype, id);

        request.source(json, XContentType.JSON);

        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);

        //处理返回
        String index = indexResponse.getIndex();
        String type = indexResponse.getType();
        String esid = indexResponse.getId();

        //拼接es链接
        //String http = "http://" + eshostname + ":" + esport + "/" + index + "/" + type + "/" + esid + "/_source";
        String esHTTP = String.format("http://%s:%s/%s/%s/%s/_source"
                ,eshostname,
                esport,
                index,
                type,
                esid
        );

        System.out.println(esHTTP);
        //client.close();
        //return esHTTP;*/

    }

    //es客户端关闭
    public static void ClientClose() {

        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
