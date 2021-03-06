package com.dinglicom.java.demo.datastream.state;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.concurrent.TimeUnit;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.StringUtils;

@SuppressWarnings("serial")
public class SourceFromFile extends RichSourceFunction<String> {
    private volatile Boolean isRunning = true;

    @Override
    public void run(SourceContext ctx) throws Exception {
        BufferedReader bufferedReader = new BufferedReader(new FileReader("E:\\documents\\test.txt"));
        while (isRunning) {
            String line = bufferedReader.readLine();
            if (StringUtils.isNullOrWhitespaceOnly(line)) {
                continue;
            }
            ctx.collect(line);
            TimeUnit.SECONDS.sleep(60);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
