package com.zqykj.tldw;

import com.zqykj.tldw.backup.service.BackupService;
import com.zqykj.tldw.service.ConsumeProcessor;
import org.springframework.beans.factory.annotation.Value;
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
public class ApplicationServer implements CommandLineRunner{

    @Value("${enable.backup}")
    private Boolean isBackUp;

    @Value("${enable.elp.transform}")
    private Boolean isElpTrans;

    public static void main(String[] args) {
        SpringApplication.run(ApplicationServer.class, args);
    }

    @Override
    public void run(String... args) throws Exception {

        if (isBackUp){
            BackupService backupService = ApplicationContextProvider.getBean("backupService", BackupService.class);
            backupService.init();
        }

        if (isElpTrans){
            ConsumeProcessor consumeProcessor = ApplicationContextProvider.getBean("consumeProcessor", ConsumeProcessor.class);
            consumeProcessor.init();
        }


    }
}
