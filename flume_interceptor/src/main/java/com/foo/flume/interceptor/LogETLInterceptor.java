package com.foo.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class LogETLInterceptor implements Interceptor {

    @Override
    public void initialize() {
    
    }

    @Override
    public Event intercept(Event event) {
        String body = new String(event.getBody(), Charset.forName("UTF-8"));

        if(LogUtils.validateReportLog(body)){
            return event;
        }

        return null;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event> intercepted = new ArrayList<>(events.size());
        for (Event event:events){
            Event interceptedEvent = intercept(event);
            if(interceptedEvent != null){
                intercepted.add(interceptedEvent);
            }
        }
       return intercepted;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
