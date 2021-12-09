package com.wwj.kafkalearn.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JsonUtil {

    @SuppressWarnings("unchecked")
    public static <U> U json2object(String json, TypeToken<U> typeToken) {
        Gson gson = new Gson();
        return (U) gson.fromJson(json, typeToken.getType());
    }

    public static <U> U json2object_gson(String json, Class<U> clazz) {
        Gson gson = new Gson();
        return gson.fromJson(json, clazz);
    }

    public static String object2jackJson(Object arg0) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(arg0);
    }


    public static String object2json(Object obj) {
        Gson gson = new Gson();
        return gson.toJson(obj);
    }


    public static <T> T parse(String json, Class<T> clazz) throws Exception {
        if (json == null || json.length() == 0) {
            return null;
        }
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return objectMapper.readValue(json, clazz);
    }

    /***
     *
     * <p>Description:[json字符串转换为bean，json中是小写下划线格式，bean中是驼峰式]</p>
     */
    public static <T> T parse2(String json, TypeReference<T> typeReference) throws Exception {
        if (json == null || json.length() == 0) {
            return null;
        }
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setPropertyNamingStrategy(com.fasterxml.jackson.databind.PropertyNamingStrategy.SNAKE_CASE);
        return objectMapper.readValue(json, typeReference);
    }

}