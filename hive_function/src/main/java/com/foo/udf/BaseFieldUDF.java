package com.foo.udf;

import groovy.json.JsonException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

public class BaseFieldUDF extends UDF {

    public  String evaluate(String line,String jsonkeyString) throws JSONException {
        StringBuilder sb = new StringBuilder();

        String[] jsonkeys = jsonkeyString.split(",");

        String[] logContents = line.split("\\|");

        if(logContents.length != 2 || StringUtils.isBlank(logContents[1])){

            return "";

        }

       try {

           JSONObject jsonObject = new JSONObject(logContents[1]);

           JSONObject base = jsonObject.getJSONObject("cm");

           for (int i = 0;i < jsonkeys.length;i++){
               String filedName = jsonkeys[i].trim();

               if (base.has(filedName)){
                   sb.append(base.getString(filedName)).append("\t");
               }else {
                   sb.append("").append("\t");
               }
           }

           sb.append(jsonObject.getString("et")).append("\t");
           sb.append(logContents[0]).append("\t");

       }catch (JsonException e){
            e.printStackTrace();
       }

        return sb.toString();
    }

}
