package com.dw.demo.graph.config;

import java.io.Serializable;

/**
 * Created by finup on 2017/11/24.
 */
public class NeoServerConfig implements Serializable{

    private String url; //neo4j服务地址
    private String user; //neo4j用户
    private String pass; //neo4j密码

    private Long sessionSize; //neo4j session数量
    private Long connectTimeout; //neo4j socket connect
    private Long transRetryTimeout; //neo4j trans

    private Long batchCount;//neo4j 批量


    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPass() {
        return pass;
    }

    public void setPass(String pass) {
        this.pass = pass;
    }

    public Long getSessionSize() {
        return sessionSize;
    }

    public void setSessionSize(Long sessionSize) {
        this.sessionSize = sessionSize;
    }

    public Long getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(Long connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public Long getTransRetryTimeout() {
        return transRetryTimeout;
    }

    public void setTransRetryTimeout(Long transRetryTimeout) {
        this.transRetryTimeout = transRetryTimeout;
    }

    public Long getBatchCount() {
        return batchCount;
    }

    public void setBatchCount(Long batchCount) {
        this.batchCount = batchCount;
    }
}
