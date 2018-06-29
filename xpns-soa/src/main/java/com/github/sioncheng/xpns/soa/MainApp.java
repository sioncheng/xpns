package com.github.sioncheng.xpns.soa;

import com.github.sioncheng.xpns.common.config.AppProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;

@SpringBootApplication
public class MainApp {

    public static void main(String[] args) throws IOException {

        AppProperties.init();

        SpringApplication.run(MainApp.class, args);
    }
}
