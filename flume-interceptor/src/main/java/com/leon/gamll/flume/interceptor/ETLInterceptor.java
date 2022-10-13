package com.leon.gamll.flume.interceptor;

import com.leon.gamll.flume.utils.JSONUtil;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.Iterator;
import java.util.List;

public class ETLInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        // 1.获取body
        byte[] eventBody = event.getBody();
        String log = new String(eventBody);
        // 2.判断数据是否完整
        if (JSONUtil.isJSONValidate(log)){
            return event;
        }else {
            return null;
        }
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        Iterator<Event> iterator = events.iterator();
        while (iterator.hasNext()){
            Event event = iterator.next();
            if (intercept(event) == null){
                iterator.remove();
            }
        }
        return events;
    }

    @Override
    public void close() {

    }

    public static class ETLBuilder implements Builder{

        @Override
        public Interceptor build() {
            return new ETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
