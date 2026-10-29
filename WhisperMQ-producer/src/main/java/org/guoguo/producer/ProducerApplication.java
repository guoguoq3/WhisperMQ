// org.guoguo.producer.ProducerApplication.java
package org.guoguo.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.guoguo.common.config.MqConfigProperties;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@EnableConfigurationProperties(MqConfigProperties.class)
@ComponentScan(basePackages = {"org.guoguo.producer", "org.guoguo.common"}) // 新增
public class ProducerApplication {
    public static void main(String[] args) {
        SpringApplication.run(ProducerApplication.class, args);
    }
}