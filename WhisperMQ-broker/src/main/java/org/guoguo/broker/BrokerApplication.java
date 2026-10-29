// org.guoguo.broker.BrokerApplication.java
package org.guoguo.broker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.guoguo.common.config.MqConfigProperties;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@EnableConfigurationProperties(MqConfigProperties.class) // 启用配置绑定
@ComponentScan(basePackages = {"org.guoguo.broker", "org.guoguo.common"})
public class BrokerApplication {
    public static void main(String[] args) {
        SpringApplication.run(BrokerApplication.class, args);
    }
}