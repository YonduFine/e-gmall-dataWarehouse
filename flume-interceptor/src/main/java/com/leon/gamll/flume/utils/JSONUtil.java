package com.leon.gamll.flume.utils;

import com.alibaba.fastjson.JSONObject;

public class JSONUtil {

    /**
     * 判断输入的JSON是否完整
     *
     * @param log 输入的JSON字符串
     * @return true or false
     */
    public static boolean isJSONValidate(String log) {
        try {
            JSONObject.parseObject(log);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static void main(String[] args) {
        System.out.println(JSONObject.parseObject("{id:1}"));
    }
}
