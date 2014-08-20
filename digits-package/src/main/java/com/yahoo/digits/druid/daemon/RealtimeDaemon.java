package com.yahoo.digits.druid.daemon;


public class RealtimeDaemon extends DruidDaemon {
    static String[] args = {"server", "realtime"};

    public RealtimeDaemon() {
        super(args, "RealtimeDaemon", RealtimeDaemon.class);
    }

}
