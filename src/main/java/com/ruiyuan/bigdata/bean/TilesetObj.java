package com.ruiyuan.bigdata.bean;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.fastjson.parser.ParserConfig;
import com.alibaba.fastjson.parser.deserializer.ObjectDeserializer;
import com.ruiyuan.bigdata.bean.nodes.LeafNode;
import com.ruiyuan.bigdata.bean.nodes.MidNode;
import com.ruiyuan.bigdata.bean.nodes.Node;
import com.ruiyuan.bigdata.bean.nodes.RootNode;
import com.ruiyuan.bigdata.utils.YamlReaderUtil;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.lang.reflect.Type;
import java.util.*;

@Slf4j
public class TilesetObj {

    private Map<String, String> asset = new HashMap<String, String>() {
        {
            put("generatetool", "cesiumlab2@www.cesiumlab.com/osgb2tiles3");
            put("gltfUpAxis", "Z");
            put("version", "1.0");
        }
    };

    private double geometricError;

    private RootNode root;

    public Map<String, String> getAsset() {
        return asset;
    }

    public void setAsset(Map<String, String> asset) {
        this.asset = asset;
    }

    public double getGeometricError() {
        return geometricError;
    }

    public void setGeometricError(double geometricError) {
        this.geometricError = geometricError;
    }

    public RootNode getRoot() {
        return root;
    }

    public void setRoot(RootNode root) {
        this.root = root;
    }

    //Root->Children的修改  传入0000的 es http
    @JSONField(serialize = false)
    public void setTileset(String jsonHttp) {
        HashMap<String, String> co = new HashMap<>();
        List<Node> children = root.getChildren();
        for (Node child : children) {
            Map<String, String> content = child.getContent();
            content.forEach((k, v) -> {
                if (v.contains("top/0_0_0_0.json")) {
                    co.put("uri", jsonHttp);
                    child.setContent(co);
                }
            });
        }
    }

    //Root->Content  b3dm
    @JSONField(serialize = false)
    public void setB3dmHdfsUri(HashMap<String, String> b3dmLocalpath_Url, String localJsonPath) {
        Map<String, String> content = root.getContent();
        if (content != null) {
            content.forEach((k, v) -> {
                if (v.endsWith("b3dm")) {
                    HashMap<String, String> co = new HashMap<>();
                    //取localJsonPath文件所在目录
                    String substring = localJsonPath.substring(0, localJsonPath.lastIndexOf("\\") + 1);
                    String b3dmAbsPath = substring + v;
                    co.put("uri", b3dmLocalpath_Url.get(b3dmAbsPath));
                    root.setContent(co);
                }
            });
        }
    }

    //1.uri
    @JSONField(serialize = false)
    public Set<String> getPntsUrl() {
        Set<String> res = new HashSet<>();
        Node root = this.getRoot();
        String rootUrl = root.getContent().get("uri");
        res.add(rootUrl);
        RootNode _root = (RootNode) root;
        if (null == _root.getChildren() || _root.getChildren().size() == 0) {
            return res;
        } else {
            getChildUrl(root, res);
            return res;
        }
    }

    //2.递归获取子节点url
    @JSONField(serialize = false)
    private void getChildUrl(Node father, Set<String> res) {
        String className = father.getClass().getSimpleName();
        if ("RootNode".equals(className)) {
            RootNode _father = (RootNode) father;
            if (null == _father.getChildren() || _father.getChildren().size() == 0) {
                return;
            } else {
                _father.getChildren().forEach(c -> {
                    res.add(c.getContent().get("uri"));
                    getChildUrl(c, res);
                    //System.out.println("RootNode "+res);
                });
            }
        }

        if ("MidNode".equals(className)) {
            MidNode _father = (MidNode) father;
            if (null == _father.getChildren() || _father.getChildren().size() == 0) {
                return;
            } else {
                _father.getChildren().forEach(c -> {
                    res.add(c.getContent().get("uri"));
                    getChildUrl(c, res);
                    //System.out.println("MidNode "+res);
                });
            }
        }
    }

    /**
     * 获取3dtiles索引json的实体对象
     *
     * @param tilesetJson
     * @return
     */
    public TilesetObj getTilesetObj(String tilesetJson) {
        ParserConfig parserConfig = new ParserConfig() {
            @Override
            public ObjectDeserializer getDeserializer(Type type) {
                if (type == Node.class) {
                    return super.getDeserializer(MidNode.class);
                }
                return super.getDeserializer(type);
            }
        };
        return JSON.parseObject(tilesetJson, TilesetObj.class, parserConfig);
    }

    //setri
    //递归修改子节点里的url
    @JSONField(serialize = false)
    public void setPntsUri(Node father, HashMap<String, String> read, String path, String inputDri) {
        //获取配置文件中es的属性
        YamlReaderUtil yamlReaderUtil = new YamlReaderUtil();
        HashMap hashMap = yamlReaderUtil.yamRead();
        String esIpAndPort = (String) hashMap.get("esIpAndPort");
        String esindex = (String) hashMap.get("esindex");
        String estype = (String) hashMap.get("estype");

        String className1 = father.getClass().getSimpleName();
        if ("RootNode".equals(className1)) {
            RootNode _father = (RootNode) father;
            //children
            List<Node> children = _father.getChildren();
            if (children != null) {
                children.forEach(k -> {
                    k.getContent().forEach((k1, v) -> {
                        //uri 里面是json
                        if (v.endsWith("json")) {
                            String z = "^[0-9a-zA-z].*";
                            if (v.matches(z) && v.contains("/")) {
                                //截取/后数据
                                v = v.replace("/", "_");
                            } else if (v.startsWith("../")) {
                                v = v.replace("../", "").replace("/", "_");
                            } else {
                                String[] split = path.split("\\\\");
                                String s = split[split.length - 2];
                                v = s + "_" + v;
                            }
                            //System.out.println("uri改后" + v);
                            HashMap<String, String> coes = new HashMap<>();
                            //输入文件最后部分数据
                            String substring = inputDri.substring(inputDri.lastIndexOf("\\") + 1);
                            //重新拼接uri                                                                               !!修改处！！
                            //String esHttp = "http://" + esIpAndPort + "/" + esindex + "/" + estype + "/" + substring + "_" + v + "/_source";
                            String esHttp = esindex + "/" + estype + "/" + substring + "_" + v + "/_source";
                            //System.out.println("rootv:josn " + esHttp);
                            coes.put("uri", esHttp);
                            //修改uri
                            k.setContent(coes);
                        }
                        //uri里面是b3dm
                        else if (v.endsWith("b3dm")) {
                            HashMap<String, String> co = new HashMap<>();
                            String substring = path.substring(0, path.lastIndexOf("\\") + 1);
                            String seqUrl = substring + v;
                            //log.info(" seqUrl:[{}]", seqUrl);
                            co.put("uri", read.get(seqUrl));
                            k.setContent(co);
                        } else {
                        }
                    });
                    setPntsUri(k, read, path, inputDri);
                });
            }
        }
        else if ("MidNode".equals(className1)) {
            MidNode _father = (MidNode) father;
            List<Node> children = _father.getChildren();
            if (children != null) {
                children.forEach(k2 -> {
                    k2.getContent().forEach((k1, v) -> {
                        if (v.endsWith("json")) {
                            HashMap<String, String> coes = new HashMap<>();
                            String z = "^[0-9a-zA-z].*";
                            if (v.matches(z) && v.contains("/")) {
                                //截取/后数据
                                v = v.replace("/", "_");
                            } else if (v.startsWith("../")) {
                                v = v.replace("../", "").replace("/", "_");
                            } else {
                                String[] split = path.split("\\\\");
                                String s = split[split.length - 2];
                                v = s + "_" + v;
                            }
                            //System.out.println("uri改后" + v);
                            String substring = inputDri.substring(inputDri.lastIndexOf("\\") + 1);
                            //                                                                                              !! 修改处 !!
                            //String esHttp = "http://" + esIpAndPort + "/" + esindex + "/" + estype + "/" + substring + "_" + v + "/_source";
                            String esHttp = esindex + "/" + estype + "/" + substring + "_" + v + "/_source";
                            //System.out.println("minv:josn " + esHttp);
                            coes.put("uri", esHttp);
                            k2.setContent(coes);
                        } else if (v.endsWith("b3dm")) {
                            HashMap<String, String> co = new HashMap<>();
                            //System.out.println("minv:b3dm " + v);
                            String substring = path.substring(0, path.lastIndexOf("\\") + 1);
                            String seqUrl = substring + v;
                            co.put("uri", read.get(seqUrl));
                            k2.setContent(co);
                        } else {
                        }
                    });
                    setPntsUri(k2, read, path, inputDri);
                });
            }
        }
        else
        {
            LeafNode _father = (LeafNode) father;
            _father.getContent().forEach((k, v) -> {
                if (v.endsWith("json")) {
                    HashMap<String, String> coes = new HashMap<>();
                    String z = "^[0-9a-zA-z].*";
                    if (v.matches(z) && v.contains("/")) {
                        //截取/后数据
                        v = v.replace("/", "_");
                    } else if (v.startsWith("../")) {
                        v = v.replace("../", "").replace("/", "_");
                    } else {
                        String[] split = path.split("\\\\");
                        String s = split[split.length - 2];
                        v = s + "_" + v;
                    }
                    //System.out.println("uri改后" + v);
                    String substring = inputDri.substring(inputDri.lastIndexOf("\\") + 1);
                    //                                                                                                      !!修改处!!
                    //String esHttp = "http://" + esIpAndPort + "/" + esindex + "/" + estype + "/" + substring + "_" + v + "/_source";
                    String esHttp = esindex + "/" + estype + "/" + substring + "_" + v + "/_source";
                    coes.put("uri", esHttp);
                    _father.setContent(coes);
                } else if (v.endsWith("b3dm")) {
                    HashMap<String, String> co = new HashMap<>();
                    String substring = path.substring(0, path.lastIndexOf("\\") + 1);
                    String seqUrl = substring + v;
                    co.put("uri", read.get(seqUrl));
                    _father.setContent(co);
                } else {
                }
            });
        }
    }
}