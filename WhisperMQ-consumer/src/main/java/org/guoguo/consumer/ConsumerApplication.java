// org.guoguo.consumer.ConsumerApplication.java
package org.guoguo.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.guoguo.common.config.MqConfigProperties;

@SpringBootApplication
@EnableConfigurationProperties(MqConfigProperties.class)
public class ConsumerApplication {
    public static void main(String[] args) {
        SpringApplication.run(ConsumerApplication.class, args);
    }
}