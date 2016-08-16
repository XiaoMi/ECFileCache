package com.xiaomi.filecache.ec;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class AppConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(AppConfig.class);

    int threadNum = 12;
    int fileSize = 64 * 1024;
    short clusterId = 20002;
    boolean getStream = false;
    String redisAddress = "localhost";

    public AppConfig(String[] args) {
        String profile = null;
        boolean readFile = false;
        if (args.length == 1) {
            profile = args[0];
        } else if (args.length == 2 && StringUtils.equals(args[0], "-f")) {
            profile = args[1];
            readFile = true;
        }

        // handle parameter
        if (StringUtils.isBlank(profile)) {
            LOGGER.info("the config property file is missing. use default args");
        }

        Properties prop = new Properties();
        InputStream inputStream = null;
        try {
            if (readFile) {
                if (!new File(profile).exists()) {
                    throw new IllegalArgumentException("the config property file does not exist: " + profile);
                }
                inputStream = new FileInputStream(profile);
            } else {
                inputStream = AppConfig.class.getResourceAsStream(profile);
            }
            prop.load(inputStream);
        } catch (FileNotFoundException e) {
            LOGGER.error("config file path is unavailable. profile: {}", profile);
            throw new IllegalArgumentException("config file path is unavailable.", e);
        } catch (IOException e) {
            LOGGER.error("found IOException while loading file. profile: {}", profile);
            throw new IllegalArgumentException("found IOException while loading file.", e);
        } finally {
            IOUtils.closeQuietly(inputStream);
        }

        threadNum = Integer.parseInt(prop.getProperty("thread.num", "1"));
        Validate.isTrue(threadNum > 0);

        fileSize= Integer.parseInt(prop.getProperty("file.size", "1024"));
        Validate.isTrue(fileSize> 0);

        clusterId = Short.parseShort(prop.getProperty("cluster.id", "20002"));
        Validate.isTrue(clusterId > 0);

        getStream = Boolean.parseBoolean(prop.getProperty("get.stream", "false"));

        redisAddress = prop.getProperty("redis.address", "localhost");

        System.out.println(toString());
        System.out.println("========== load config finished =============");
    }

    @Override
    public String toString() {
        return "AppConfig{" +
                "threadNum=" + threadNum +
                ", fileSize=" + fileSize +
                ", clusterId=" + clusterId +
                ", getStream=" + getStream +
                '}';
    }
}
