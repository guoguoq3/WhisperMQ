package org.guoguo.console;

import org.guoguo.common.config.MqConfigProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;

/**
 * WhisperMQ 本地 Web 控制台。
 *
 * <p>这个进程同时扮演三个角色：
 * <ol>
 *   <li>内嵌 Broker（扫描 org.guoguo.broker.*，默认监听 9999）；</li>
 *   <li>Web 服务（默认 8080），提供控制台页面与 REST API；</li>
 *   <li>内部再创建生产者 / 消费者客户端实例，走真实 TCP 协议连自己的 Broker，
 *       所以页面上的操作与真实使用完全一致，不是假数据。</li>
 * </ol>
 *
 * <p>只扫描 broker 的具体子包，避免把 {@link org.guoguo.broker.BrokerApplication}
 * （另一个 @SpringBootApplication）当作配置类扫进来。</p>
 */
@SpringBootApplication
@EnableConfigurationProperties(MqConfigProperties.class)
@ComponentScan(basePackages = {
        "org.guoguo.console",
        "org.guoguo.common",
        "org.guoguo.broker.core",
        "org.guoguo.broker.handler",
        "org.guoguo.broker.util",
        "org.guoguo.broker.store",
        "org.guoguo.broker.ConsumerGroup"
})
public class ConsoleApplication {

    public static void main(String[] args) {
        SpringApplication.run(ConsoleApplication.class, args);
    }
}
