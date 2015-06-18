package com.xiaomi.filecache.ec;

public class AppMain {

    public static void main(String[] args) {
        AppConfig appConfig = new AppConfig(args); // args[0] == "/app.conf"
        FileCachePerf fileCachePerf = new FileCachePerf(appConfig);
        fileCachePerf.run();
    }
}
