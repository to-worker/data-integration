package com.zqykj.tldw;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * @author feng.wei
 * @date 5/8/2018
 */
@SpringBootApplication
@EnableScheduling
@ComponentScan({ "com.zqykj" })
public class ApplicationServer implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(ApplicationServer.class, args);
    }

    @Override
    public void run(String... strings) throws Exception {
        System.out.println("ApplicationServer....");
    }
}
