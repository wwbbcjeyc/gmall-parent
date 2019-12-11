package com.xbgh.gmall.publisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.xbgh.gmall.publisher.mapper")
public class PublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(PublisherApplication.class, args);
    }

}
