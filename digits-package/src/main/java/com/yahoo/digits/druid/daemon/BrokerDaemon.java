package com.yahoo.digits.druid.daemon;


public class BrokerDaemon extends DruidDaemon {
    static String[] args = {"server", "broker"};

    public BrokerDaemon() {
        super(args, "BrokerDaemon", BrokerDaemon.class);
    }
}
