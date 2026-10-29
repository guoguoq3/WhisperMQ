package org.guoguo.console.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 控制台自身配置（与 MQ 配置 whisper-mq.* 分开）。
 */
@Data
@Component
@ConfigurationProperties(prefix = "whisper-console")
public class ConsoleProperties {

    /** “运行 Maven 测试”按钮执行的命令，在仓库根目录执行 */
    private String mavenCommand = "mvn -o test";

    /** 仓库根目录，留空则自动向上查找含 &lt;modules&gt; 的 pom.xml */
    private String repoRoot = "";

    /** 控制台展示的最近消息条数 */
    private int maxRecentMessages = 30;
}
