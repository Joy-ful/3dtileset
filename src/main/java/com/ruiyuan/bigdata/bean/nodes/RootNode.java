package com.ruiyuan.bigdata.bean.nodes;

import java.util.List;

public class RootNode extends Node{
    List<Node> children;
    List<Double> transform;

    public List<Node> getChildren() {
        return children;
    }

    public void setChildren(List<Node> children) {
        this.children = children;
    }

    public List<Double> getTransform() {
        return transform;
    }

    public void setTransform(List<Double> transform) {
        this.transform = transform;
    }
}
