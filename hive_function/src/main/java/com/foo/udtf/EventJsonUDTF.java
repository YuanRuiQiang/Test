package com.foo.udtf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;
public class EventJsonUDTF extends GenericUDTF {


    public StructObjectInspector initialize(){
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        fieldNames.add("event_name");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("event_json");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOIs);
    }
    @Override
    public void process(Object[] objects) throws HiveException {
        String input  = objects[0].toString();

        if(StringUtils.isBlank(input)){
            return;
        }else {
            try {
                JSONArray ja = new JSONArray(input);
                if(ja == null){
                    return;
                }
                for(int i=0;i<ja.length();i++){
                    String[] result = new String[2];
                    try {
                        result[0] = ja.getJSONObject(i).getString("en");

                        result[1] = ja.getString(i);
                    }catch (JSONException e){
                        continue;
                    }
                    forward(result);
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
