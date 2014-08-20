package com.yahoo.digits.druid.daemon;


public class CoordinatorDaemon extends DruidDaemon {
    static String[] args = {"server", "coordinator"};

    public CoordinatorDaemon() {
        super(args, "CoordinatorDaemon", CoordinatorDaemon.class);
    }

}
