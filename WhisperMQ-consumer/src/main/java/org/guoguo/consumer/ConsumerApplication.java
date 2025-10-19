// org.guoguo.consumer.ConsumerApplication.java
package org.guoguo.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.guoguo.common.config.MqConfigProperties;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@EnableConfigurationProperties(MqConfigProperties.class)
@ComponentScan(basePackages = {"org.guoguo.consumer", "org.guoguo.common"}) // 新增
public class ConsumerApplication {
    public static void main(String[] args) {
        SpringApplication.run(ConsumerApplication.class, args);
    }
}