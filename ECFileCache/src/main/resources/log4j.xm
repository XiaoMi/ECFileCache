<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE log4j:configuration SYSTEM "log4j.dtd">
<log4j:configuration xmlns:log4j="http://jakarta.apache.org/log4j/">
    <appender name="stdout" class="org.apache.log4j.ConsoleAppender">
        <param name="Target" value="System.out"/>
        <layout class="org.apache.log4j.PatternLayout">
            <param name="ConversionPattern" value="[%-5p %d{yyyy-MM-dd HH:mm:ss.SSS}] %l [%m]%n"/>
        </layout>
    </appender>

    <appender name="file" class="org.apache.log4j.DailyRollingFileAppender">
        <param name="File" value="/data/soft/resin/log/xmss-filecache-ec.log"/>
        <param name="DatePattern" value=".yyyyMMdd"/>
        <layout class="org.apache.log4j.PatternLayout">
            <param name="ConversionPattern" value="[%-5p %d{yyyy-MM-dd HH:mm:ss.SSS}] %l [%m]%n"/>
        </layout>
    </appender>

    <logger name="com.xiaomi" additivity="false">
        <level value="${log4j_level}"/>
        <appender-ref ref="file"/>
    </logger>

    <root>
        <level value="${log4j_level}"/>
        <appender-ref ref="file"/>
    </root>

</log4j:configuration>
