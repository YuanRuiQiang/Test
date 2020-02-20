package com.foo.flume.interceptor;

import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class LogUtils {
    private static Logger logger = LoggerFactory.getLogger(LogUtils.class);

    public static boolean validateReportLog(String log){
        try {
            if(log.split("\\|").length<2){
                return false;
            }

            if(log.split("\\|")[0].length()!=13 || !NumberUtils.isDigits(log.split("\\|")[1])){
                return false;
            }
            if(!log.split("\\|")[1].trim().startsWith("{") || !log.split("\\|")[1].trim().endsWith("}")){
                return false;
            }
        }catch (Exception e){
            logger.error("error parse,message is:"+log);
            logger.error(e.getMessage());
            return false;
        }
        return true;
    }


}
