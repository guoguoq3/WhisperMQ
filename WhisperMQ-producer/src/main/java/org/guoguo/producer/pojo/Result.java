package org.guoguo.producer.pojo;

import lombok.Data;
import org.guoguo.producer.constant.ResultCodeEnum;

/**
 * 全局统一返回结果类
 *
 */
@Data
public class Result<T> {
    // 返回码
    private Integer code;
    /**
     * 消息唯一标识
     */
    private String messageId;
    // 返回消息
    private String message;
    // 返回数据
    private T data;
//    public Result(){}
    // 返回数据
    protected static <T> Result<T> build(T data) {
        Result<T> result = new Result<T>();
        if (data != null)
            result.setData(data);
        return result;
    }
    public static <T> Result<T> build(T body, Integer code, String message, String messageId) {

        Result<T> result = build(body);
        result.setMessageId(messageId);
        result.setCode(code);
        result.setMessage(message);
        return result;
    }

    public static <T> Result<T> build(T body, ResultCodeEnum resultCodeEnum, String messageId) {
        Result<T> result = build(body);
        result.setMessageId(messageId);
        result.setCode(resultCodeEnum.getCode());
        result.setMessage(resultCodeEnum.getMessage());
        return result;
    }
    /**
     * 操作成功
     * @param data  baseCategory1List
     * @param <T>
     * @return
     */
    public static<T> Result<T> ok(T data,String messageId){
        Result<T> result = build(data);
        return build(data, ResultCodeEnum.SUCCESS,messageId);
    }
    public Result<T> message(String msg){
        this.setMessage(msg);
        return this;
    }
    public Result<T> code(Integer code){
        this.setCode(code);
        return this;
    }
}