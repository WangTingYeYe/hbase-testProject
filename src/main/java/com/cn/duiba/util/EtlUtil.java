package com.cn.duiba.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;

import java.math.BigDecimal;
import java.util.*;

public class EtlUtil {
    public static String getMD5HbaseRowkey(String key) {
        return MD5Util.computeMD5(key).substring(0, 4) + "-" + key;
    }

    public static String getMD5HbaseRowkey(String... str) {
        return getMD5HbaseRowkey(getRedisKey(str));
    }


    public static String getRedisKey(String... strs) {
        if (strs.length == 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (String str : strs) {
            sb.append(str).append("_");
        }
        return sb.toString().substring(0, sb.toString().length() - 1);
    }

    //解析json数据
    public static JSONObject parseJson(Object content) {
        JSONObject jsonObject = null;
        if (content == null) {
            return null;
        }
        if (content instanceof String) {
            String json = (String) content;
            jsonObject = JSON.parseObject(json);
        } else if (content instanceof JSONObject) {
            jsonObject = (JSONObject) content;
        }
        return jsonObject;
    }

    public static String getString(JSONObject object, String key) {
        if (object == null) {
            return null;
        }
        String str = object.getString(key);
        if (StringUtils.isBlank(str)) {
            return null;
        }
        if ("null".equalsIgnoreCase(str)) {
            return null;
        }
        return str;
    }
}
