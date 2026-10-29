package org.guoguo.common.protocol;

import com.alibaba.fastjson.JSON;
import org.guoguo.common.pojo.DTO.RpcMessageDTO;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 协议版本（P1）：旧客户端不传 version 时按 1 处理，未知字段忽略。
 */
class ProtocolVersionTest {

    @Test
    void missingVersionDefaultsToCurrent() {
        RpcMessageDTO dto = JSON.parseObject(
                "{\"request\":true,\"methodType\":\"P_SEND_MSG\",\"json\":\"{}\"}",
                RpcMessageDTO.class);

        assertNotNull(dto);
        assertEquals(RpcMessageDTO.PROTOCOL_VERSION, dto.getVersion());
    }

    @Test
    void versionRoundTrips() {
        RpcMessageDTO dto = new RpcMessageDTO();
        dto.setVersion(RpcMessageDTO.PROTOCOL_VERSION);
        dto.setMethodType("P_SEND_MSG");

        RpcMessageDTO back = JSON.parseObject(JSON.toJSONString(dto), RpcMessageDTO.class);

        assertEquals(RpcMessageDTO.PROTOCOL_VERSION, back.getVersion());
        assertTrue(JSON.toJSONString(dto).contains("\"version\":1"));
    }

    @Test
    void tokenRoundTrips() {
        RpcMessageDTO dto = new RpcMessageDTO();
        dto.setToken("secret-token");

        RpcMessageDTO back = JSON.parseObject(JSON.toJSONString(dto), RpcMessageDTO.class);

        assertEquals("secret-token", back.getToken());
        assertTrue(JSON.toJSONString(dto).contains("\"token\""));
    }

    @Test
    void unknownFutureFieldsAreIgnored() {
        RpcMessageDTO dto = JSON.parseObject(
                "{\"version\":1,\"request\":false,\"methodType\":\"X\",\"json\":\"{}\","
                        + "\"futureField\":{\"a\":1},\"another\":[1,2]}",
                RpcMessageDTO.class);

        assertEquals("X", dto.getMethodType());
        assertEquals(1, dto.getVersion());
    }
}
