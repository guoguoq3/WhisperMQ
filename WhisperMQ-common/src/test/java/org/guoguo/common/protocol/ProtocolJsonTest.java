package org.guoguo.common.protocol;

import com.alibaba.fastjson.JSON;
import org.guoguo.common.constant.DeadType;
import org.guoguo.common.constant.MethodType;
import org.guoguo.common.pojo.DTO.DeadLetterDTO;
import org.guoguo.common.pojo.DTO.RpcMessageDTO;
import org.guoguo.common.pojo.DTO.SubscribeReqDTO;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * 协议 JSON 往返：信封、功能层 DTO、新增方法类型。
 */
class ProtocolJsonTest {

    @Test
    void rpcEnvelopeRoundTripKeepsMethodTypeAndDirection() {
        RpcMessageDTO rpc = new RpcMessageDTO();
        rpc.setRequest(false);
        rpc.setMethodType(MethodType.ERROR);
        rpc.setJson("{\"k\":\"v\"}");

        RpcMessageDTO back = JSON.parseObject(JSON.toJSONString(rpc), RpcMessageDTO.class);

        assertNotNull(back);
        assertEquals(MethodType.ERROR, back.getMethodType());
        assertFalse(back.isRequest());
        assertEquals("{\"k\":\"v\"}", back.getJson());
    }

    @Test
    void subscribeRequestRoundTripKeepsTraceIdAndTags() {
        SubscribeReqDTO req = new SubscribeReqDTO();
        req.setTopic("ORDER_TOPIC");
        req.setGroupId("ORDER_GROUP");
        req.setTags(List.of("TAG1", "TAG2"));

        SubscribeReqDTO back = JSON.parseObject(JSON.toJSONString(req), SubscribeReqDTO.class);

        assertEquals(req.getMessageId(), back.getMessageId(), "traceId 必须原样往返，订阅确认靠它关联");
        assertEquals("ORDER_TOPIC", back.getTopic());
        assertEquals(List.of("TAG1", "TAG2"), back.getTags());
    }

    @Test
    void deadLetterRoundTripKeepsTypeAndRetryCount() {
        DeadLetterDTO dto = new DeadLetterDTO();
        dto.setMessageId("origin-1");
        dto.setDeadType(DeadType.RETRY_EXHAUSTED);
        dto.setDeadRetryCount(8);

        DeadLetterDTO back = JSON.parseObject(JSON.toJSONString(dto), DeadLetterDTO.class);

        assertEquals(dto.getDeadLetterId(), back.getDeadLetterId());
        assertEquals(DeadType.RETRY_EXHAUSTED, back.getDeadType());
        assertEquals(8, back.getDeadRetryCount());
        assertNotNull(back.getDeadTime(), "deadTime 必须能往返");
    }

    @Test
    void newMethodTypesAreDistinct() {
        assertNotEquals(MethodType.HEARTBEAT_PING, MethodType.HEARTBEAT_PONG);
        assertNotEquals(MethodType.B_SUBSCRIBE_RESPONSE, MethodType.B_CONTROL_RESPONSE);
        assertNotEquals(MethodType.ERROR, MethodType.DEAD_LETTER_REPLAY);
    }
}
