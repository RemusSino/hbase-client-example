/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.rsino.hbaseexample;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 *
 * @author rsinorchian
 */
public class HBaseManager {

    private String addr;
    private String port;
    private static Configuration config;

    private HBaseManager(String addr, String port) {
        this.addr = addr;
        this.port = port;
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", addr);// ip of hbase server(remote)
        config.set("hbase.zookeeper.property.clientPort", port);// portno : 2181 default
    }

    public synchronized static Configuration getConfig(String addr, String port) {
        if (config == null) {
            HBaseManager cm = new HBaseManager(addr, port);
        }
        return config;
    }
}
