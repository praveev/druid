package com.yahoo.druid;

import io.druid.cli.Main;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;

/**
 * See http://twiki.corp.yahoo.com/view/JavaPlatform/PackageYJavaDaemon
 */
public class DruidDaemon implements Daemon {

    
    private String[] args;

    @Override
    public void init(DaemonContext context) throws Exception {
        System.out.println("Druid Daemon Initializing...");
        args = context.getArguments();
        System.out.println("Druid Daemon Initialization done.");
    }

    @Override
    public void start() throws Exception {
        System.out.println("Druid Daemon starting now....");
        Main.main(args);
        System.out.println("Druid Daemon started.");
    }

    @Override
    public void stop() throws Exception {
    }

    @Override
    public void destroy() {
    }
}
