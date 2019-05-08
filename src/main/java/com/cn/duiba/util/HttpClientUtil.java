package com.cn.duiba.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class HttpClientUtil {
	private static final Logger logger = LoggerFactory.getLogger(HttpClientUtil.class);
	private static RequestConfig requestConfig = RequestConfig.custom()
			.setConnectTimeout(10000)
			.setConnectionRequestTimeout(10000)  
            .setSocketTimeout(120000)
            .build();
	
	public static void main(String[] args) {
		String httpUrl = "http://configserver.dui88.com/duiba_jstorm/test";
		Map<String, String> map = new HashMap<>();
		map.put("metricKey", "consumerOrderFeature-totalDelayCount");
		map.put("value", "10");
		String response = httpGet(httpUrl, map);
		JSONObject jsonObject = EtlUtil.parseJson(response);
		JSONArray jsonArray = JSON.parseArray(EtlUtil.getString(jsonObject, "propertySources"));
		for (Object o : jsonArray) {
			JSONObject json = EtlUtil.parseJson(o);
			if("apollo.test".equals( EtlUtil.getString(json,"name"))){
				System.out.println(EtlUtil.getString(json,"source"));

			}
		}
	}
	
	public static String httpGet(String httpUrl, Map<String, String> paramMap){
		CloseableHttpClient httpClient = null;  
        CloseableHttpResponse response = null; 
        try {
			if(paramMap != null && paramMap.size() > 0){
				StringBuilder sb = new StringBuilder();
				sb.append(httpUrl).append("?");
				for(String key : paramMap.keySet()){
					String value = paramMap.get(key);
					sb.append(key).append("=").append(value).append("&");
				}
				sb.deleteCharAt(sb.length() - 1);
				httpUrl = sb.toString();
			}
			httpClient = HttpClients.createDefault();
			HttpGet httpGet = new HttpGet(httpUrl);
			//执行请求
			response = httpClient.execute(httpGet);
			return EntityUtils.toString(response.getEntity(), "UTF-8");
		} catch (Exception e) {
			logger.error("http get error.", e);
		} finally{
			try {
				if(response != null){
					response.close();
				}
				if(httpClient != null){
					httpClient.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
		return null;
	}
	
	public static String httpPost(String httpUrl, String content){
		CloseableHttpClient httpClient = null;  
        CloseableHttpResponse response = null;  
        try {
        	//创建httpPost
    		HttpPost httpPost = new HttpPost(httpUrl);
    		if(StringUtils.isNotBlank(content)){
    			StringEntity stringEntity = new StringEntity(content, "UTF-8");
    			stringEntity.setContentType("application/json");
    			httpPost.setEntity(stringEntity);
    		}
    		httpClient = HttpClients.createDefault();
    		httpPost.setConfig(requestConfig);
    		//执行请求
    		response = httpClient.execute(httpPost);
    		return EntityUtils.toString(response.getEntity(), "UTF-8");
		} catch (Exception e) {
			logger.error("http post error.", e);
		} finally{
			try {
				if(response != null){
					response.close();
				}
				if(httpClient != null){
					httpClient.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
		return null;
	}
}
