package org.guoguo.producer.constant;


import lombok.Getter;

@Getter
public enum ResultCodeEnum {
    SUCCESS(200, "发送成功"),
    FAILED(501, "发送失败"),
    ;

    private Integer code;
    private String message;
    private ResultCodeEnum(Integer code, String message) {
        this.code = code;
        this.message = message;
    }
    public Integer getCode() {
        return code;
    }
    public String getMessage() {
        return message;
    }

}
