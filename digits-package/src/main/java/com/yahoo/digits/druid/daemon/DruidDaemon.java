package com.yahoo.digits.druid.daemon;

import io.druid.cli.Main;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.digits.druid.util.Configuration;

public class DruidDaemon implements Daemon {

    protected Logger log;
    protected static final int RESTART_STATUS_CODE = 123;
    protected String[] args;
    protected String component;


    Configuration configuration = new Configuration();

    public DruidDaemon() {
        super();
    }

    public DruidDaemon(String[] args, String component, Class daemonclass) {
        this.args = args;
        this.component = component;
        log = LoggerFactory.getLogger(daemonclass);
    }

    @Override
    public void init(DaemonContext context) throws Exception {
        log.info(component + "::init");

        // setDefaultUncaughtExceptionHandler could be called again in child classes to
        // change the UncaughtExceptionHandler if required
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                log.error("Restarting after error: " + e.getMessage(), e);
                System.exit(RESTART_STATUS_CODE);
            }
        });

        configuration = new Configuration();
        // TODO Add YKEYKEY code here for password. Add it to
    }

    @Override
    public void start() throws Exception {
        log.info("Enter " + component + "::start.");

        Main.main(args);

        log.info("Exit " + component + "::start");
    }

    @Override
    public void stop() throws Exception {
        log.info("Enter " + component + "::stop");
    }

    @Override
    public void destroy() {
        log.info("Enter " + component + "::destroy");
    }

}
