package org.guoguo.common.pojo.Entity;

import lombok.Getter;
import org.guoguo.common.util.SnowflakeIdGeneratorUtil;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author 荆锦硕
 * @date 2025年09月13日 12:56
 * <p>
 * description:定义带版本的Latch包装类
 */

public class VersionedLatch extends FunctionEntity{
    @Getter
    private final CountDownLatch latch;
    @Getter
    private final long  version;


    public VersionedLatch(int count){
        // 1. 生成唯一版本号（此处简化为直接使用雪花算法）
        this.version = SnowflakeIdGeneratorUtil.getInstance().nextId();
        // 2. 创建新的latch并存储，（确保新重试与旧latch彻底隔离）
        this.latch = new CountDownLatch(count);
    }
    public void countDown(long version){
        if (this.version == version) {
            // 验证版本号一致性
            this.latch.countDown();
        }
    }
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        return this.latch.await(timeout,unit);
    }




}
