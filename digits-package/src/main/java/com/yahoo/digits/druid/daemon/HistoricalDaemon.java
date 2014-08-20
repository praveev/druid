package com.yahoo.digits.druid.daemon;


public class HistoricalDaemon extends DruidDaemon {
    static String[] args = {"server", "historical"};

    public HistoricalDaemon() {
        super(args, "HistoricalDaemon", HistoricalDaemon.class);
    }
}
