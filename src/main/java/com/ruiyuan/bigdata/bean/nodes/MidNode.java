package com.ruiyuan.bigdata.bean.nodes;

import java.util.List;

public class MidNode extends Node{
    List<Node> children;

    public List<Node> getChildren() {
        return children;
    }

    public void setChildren(List<Node> children) {
        this.children = children;
    }
}
