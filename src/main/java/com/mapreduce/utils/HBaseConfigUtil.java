package com.mapreduce.utils;

import com.mapreduce.constants.Connections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class HBaseConfigUtil {

    public static Configuration createHBaseConfiguration(String zookeeperQuorum, String clientPort, String znodeParent) {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", zookeeperQuorum);
        config.set("hbase.zookeeper.property.clientPort", clientPort);
        config.set("zookeeper.znode.parent", znodeParent);
        return config;
    }

    public static Configuration getSIConfiguration(String zookeeperQuorum, String clientPort) {
        return createHBaseConfiguration(zookeeperQuorum, clientPort, Connections.ZOOKEEPER_ZNODE_PARENT);
    }
}

