package com.github.sioncheng.xpns.server.akka;

import akka.http.javadsl.model.HttpEntity;
import akka.http.javadsl.model.MediaTypes;
import akka.http.javadsl.unmarshalling.Unmarshaller;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;


public class FastjsonUnmarshller  {

   public static Unmarshaller<HttpEntity, JSONObject> unmarshaller()   {
       return Unmarshaller.forMediaType(MediaTypes.APPLICATION_JSON, Unmarshaller.entityToString()).thenApply(jsonString -> JSON.parseObject(jsonString));
   }


    public static <T> Unmarshaller<HttpEntity, T> unmarshaller(Class<T> tClass)   {
        return Unmarshaller.forMediaType(MediaTypes.APPLICATION_JSON, Unmarshaller.entityToString()).thenApply(jsonString -> JSON.parseObject(jsonString,tClass));
    }
}
