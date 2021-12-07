package com.ruiyuan.bigdata.bean.nodes;

import java.util.List;
import java.util.Map;

/**
 * @Description
 */
public abstract class Node {
    double geometricError;
    Map<String,List<Double>> boundingVolume;
    Map<String,String> content;

    public Map<String, List<Double>> getBoundingVolume() {
        return boundingVolume;
    }

    public void setBoundingVolume(Map<String, List<Double>> boundingVolume) {
        this.boundingVolume = boundingVolume;
    }

    public Map<String, String> getContent() {
        return content;
    }

    public void setContent(Map<String, String> content) {
        this.content = content;
    }

    public double getGeometricError() {
        return geometricError;
    }


    public void setGeometricError(double geometricError) {
        this.geometricError = geometricError;
    }
}
