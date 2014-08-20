package com.yahoo.digits.druid.daemon;


public class IndexerDaemon extends DruidDaemon {
    static String[] args = {"server", "overlord"};

    public IndexerDaemon() {
        super(args, "IndexerDaemon", IndexerDaemon.class);
    }

}
