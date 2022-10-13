package com.leon.gamll.flume.interceptor;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class TimestampInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    /**
     * 将log的产生时间放入headers,解决时间戳偏移问题
     *
     * @param event Event to be intercepted
     * @return event
     */
    @Override
    public Event intercept(Event event) {
        // 1.获取headers
        Map<String, String> headers = event.getHeaders();
        // 2.获取body
        byte[] body = event.getBody();
        String log = new String(body, StandardCharsets.UTF_8);
        // 3.将 log 解析为JSON对象
        JSONObject logJson = JSONObject.parseObject(log);
        // 4.获取log产生时间并放入headers
        String ts = logJson.getString("ts");
        headers.put("timestamp", ts);
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {

    }

    public static class TsBuilder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new TimestampInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
