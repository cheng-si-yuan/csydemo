package com.csy;

import com.alibaba.fastjson2.JSONObject;

import java.util.HashMap;

public class demo1 {
    public static void main(String[] args) {
        HashMap<Object, Object> map = new HashMap<>();
        map.put("3214","5334b54");
        map.put("43r","657h");
        map.put("23523","34gq3tq");
        System.out.println(JSONObject.toJSONString(map));
    }
}
