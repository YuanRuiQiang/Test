package com.foo.flume.interceptor;



import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LogTypeInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        //1:获取flume接受信息头
        Map<String,String> headers = event.getHeaders();
        //2:获取flume接受的json数据数组
        byte[] json = event.getBody();
        //将json数组转换为字符串
        String jsonStr = new String(json);

        String logTyep = "";
        //startlog
        if (jsonStr.contains("start")){
            logTyep = "start";
        }
        //evenLog
        else {
            logTyep = "event";
        }
        //3:将日志类型存储到flume头中
        headers.put("logType",logTyep);
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        ArrayList<Event> interceptors = new ArrayList<>();

        for (Event event : list){
            Event interceptEvent = intercept(event);
            interceptors.add(interceptEvent);
        }
        return interceptors;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogTypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
