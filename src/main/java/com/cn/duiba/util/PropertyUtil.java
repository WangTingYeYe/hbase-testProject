package com.cn.duiba.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

public class PropertyUtil {
    private static final Logger logger = LoggerFactory.getLogger(PropertyUtil.class);
    //volatile 禁止jvm进行指令重排
    private volatile static PropertyUtil instance = null;
    private JSONObject props;
    private static final String TEST_URL = "http://configserver.dui88.com/duiba_jstorm/test";
    private static final String PRODVPC_URL = "http://configserver.duiba.com.cn/duiba_jstorm/prodvpc";


    public PropertyUtil() {
    }

    public static void main(String[] args) {
        PropertyUtil prodvpc = PropertyUtil.getInstance("prodvpc");
        System.out.println( prodvpc.getProperty("credits.datasource.url"));
    }

    private PropertyUtil(String environment) {
        String propUrl = "";
        switch (environment){
            case "test":
                propUrl = TEST_URL;
                break;
            case "prodvpc":
                propUrl = PRODVPC_URL;
                break;
        }
        String response = "{\"name\":\"duiba_jstorm\",\"profiles\":[\"test\"],\"label\":null,\"version\":\"b750c1f5ea9b9d4d2f41fac44d8bab32\",\"state\":null,\"propertySources\":[{\"name\":\"apollo.test\",\"source\":{\"credits.datasource.url\":\"jdbc:mysql://10.170.1.131:3306/credits?useUnicode=true&characterEncoding=UTF-8\",\"redis.port\":\"6379\",\"dbback.datasource.url\":\"jdbc:mysql://10.170.1.131:3306/db_back?useUnicode=true&characterEncoding=UTF-8\",\"duiba.redis.auth\":\"dbseccodeNrNQ9dVVWzU\",\"mongodb_maxwaitthread\":\"10\",\"aly.es.cluster.name\":\"es-cn-4590zzqqm000luhov\",\"advert_statistics.datasource.password\":\"dbseccode8LtcAhhAk4iKbW\",\"duiba.redis.host\":\"test.config.dui88.com\",\"tuia.redis.auth\":\"dbseccodeNrNQ9dVVWzU\",\"redis.test_on_borrow\":\"false\",\"mongodb_maxtimeout\":\"60\",\"news.datasource.url\":\"jdbc:mysql://10.170.1.131:3306/tuia_news?useUnicode=true&characterEncoding=UTF-8\",\"tuia.redis.host\":\"test.config.dui88.com\",\"dbback.datasource.password\":\"dbseccode8LtcAhhAk4iKbW\",\"advert_statistics.datasource.username\":\"dev\",\"mongodb_maxwaittime\":\"60\",\"nezha.redis.auth\":\"dbseccodeNrNQ9dVVWzU\",\"aly.es.host\":\"es-cn-4590zzqqm000luhov.public.elasticsearch.aliyuncs.com\",\"advert.hbase.zklist\":\"10.170.1.153:2181,10.170.1.154:2181,10.170.1.155:2181\",\"goods.datasource.username\":\"dev\",\"advert_statistics.datasource.url\":\"jdbc:mysql://10.170.1.131:3306/advert_statistics?useUnicode=true&characterEncoding=UTF-8\",\"news.datasource.username\":\"dev\",\"mongodb_hostports\":\"172.16.80.51:3717\",\"qiho.datasource.password\":\"dbseccode8LtcAhhAk4iKbW\",\"druidConfig.datasource.password\":\"dbseccode8LtcAhhAk4iKbW\",\"alg.redis.auth\":\"dbseccodeNrNQ9dVVWzU\",\"aly.es.test.username\":\"elastic\",\"qiho.datasource.url\":\"jdbc:mysql://10.170.1.131:3306/qiho?useUnicode=true&characterEncoding=UTF-8\",\"redis.timeout\":\"10000\",\"nezha.redis.host\":\"test.config.dui88.com\",\"tuia_activity.datasource.password\":\"dbseccode8LtcAhhAk4iKbW\",\"aly.es.test.host\":\"es-cn-0pp0o3vri001fu39m.public.elasticsearch.aliyuncs.com\",\"druid.metadata.storage.type\":\"mysql\",\"dmp_data.datasource.username\":\"dev\",\"druidConfig.datasource.url\":\"jdbc:mysql://10.170.1.131:3306/druid_config?useUnicode=true&characterEncoding=UTF-8\",\"news.datasource.password\":\"dbseccode8LtcAhhAk4iKbW\",\"devDB.datasource.url\":\"jdbc:mysql://10.170.1.131:3306/dmp_data?useUnicode=true&characterEncoding=UTF-8\",\"credits.datasource.username\":\"dev\",\"dmp_data.metadata.storage.type\":\"mysql\",\"dmp_data.datasource.url\":\"jdbc:mysql://10.170.1.131:3306/dmp_data\",\"goods.datasource.password\":\"dbseccode8LtcAhhAk4iKbW\",\"alg.redis.host\":\"10.170.1.131\",\"redis.max_active\":\"10\",\"tuia_activity.datasource.username\":\"dev\",\"quwen.redis.auth\":\"dbseccodeNrNQ9dVVWzU\",\"credits.datasource.password\":\"dbseccode8LtcAhhAk4iKbW\",\"es.test.cluster.name\":\"test-es\",\"redis.max_wait\":\"10000\",\"druid.datasource.url\":\"jdbc:mysql://10.170.1.131:3306/druid\",\"aly.es.test.pwd\":\"PbnaN@ErrmT%2z6m4ULRU22Dw6\",\"mongodb_username\":\"tuia-test\",\"es.hostsports\":\"172.16.1.121:9300,172.16.1.122:9300,172.16.1.123:9300\",\"es.test.hostsports\":\"172.16.1.141:9300,172.16.1.142:9300,172.16.1.143:9300\",\"devDB.datasource.username\":\"dev\",\"redis.test_while_idle\":\"90000\",\"druid.datasource.password\":\"dbseccode8LtcAhhAk4iKbW\",\"tuia_adver.datasource.password\":\"dbseccode8LtcAhhAk4iKbW\",\"tuia_activity.datasource.url\":\"jdbc:mysql://10.170.1.131:3306/tuia_activity?useUnicode=true&characterEncoding=UTF-8\",\"tuia_media.redis.host\":\"10.170.1.131\",\"tuia_media.redis.auth\":\"dbseccodeNrNQ9dVVWzU\",\"redis.min_idle\":\"1\",\"aly.es.test.cluster.name\":\"es-cn-0pp0o3vri001fu39m\",\"oldtuia.hbase.zklist\":\"10.170.1.153:2181,10.170.1.154:2181,10.170.1.155:2181\",\"dmp_data.datasource.password\":\"dbseccode8LtcAhhAk4iKbW\",\"redis.max_idle\":\"5\",\"quwen.redis.host\":\"10.170.1.131\",\"hadoop.hbase.zklist\":\"10.170.1.153:2181,10.170.1.154:2181,10.170.1.155:2181\",\"mongodb_database\":\"tuia-test\",\"tuia.hbase.zklist\":\"10.170.1.153:2181,10.170.1.154:2181,10.170.1.155:2181\",\"es.cluster.name\":\"bigdata-es\",\"devDB.datasource.password\":\"dbseccode8LtcAhhAk4iKbW\",\"druid.datasource.username\":\"dev\",\"dev.redis.host\":\"10.170.1.131\",\"tuia_adver.datasource.username\":\"dev\",\"aly.es.username\":\"elastic\",\"risk.hbase.zklist\":\"10.170.1.153:2181,10.170.1.154:2181,10.170.1.155:2181\",\"hadoop.redis.host\":\"10.170.1.131\",\"youtui.redis.auth\":\"dbseccodeNrNQ9dVVWzU\",\"mongodb_passwd\":\"dbseccodeHgWgWEdXkmpzHtFzfPNUrP\",\"druidConfig.datasource.username\":\"dev\",\"newtuia.hbase.zklist\":\"10.170.1.153:2181,10.170.1.154:2181,10.170.1.155:2181\",\"goods.datasource.url\":\"jdbc:mysql://10.170.1.131:3306/goods?useUnicode=true&characterEncoding=UTF-8\",\"youtui.redis.host\":\"10.170.1.131\",\"mongodb_maxactive\":\"10\",\"hadoop.redis.auth\":\"dbseccodeNrNQ9dVVWzU\",\"tuia_adver.datasource.url\":\"jdbc:mysql://10.170.1.131:3306/tuia_adver?useUnicode=true&characterEncoding=UTF-8\",\"aly.es.pwd\":\"dbseccode6buNW6UU16MqT9b6TH1o9Ljn8Z1QbYHTmfgT\",\"dbback.datasource.username\":\"dev\",\"qiho.datasource.username\":\"dev\"}},{\"name\":\"http://gitlab2.dui88.com/credits-group/dev-profile.git/application-test.properties\",\"source\":{\"spring.sleuth.sampler.probability\":\"1\",\"feign.compression.response.enabled\":\"true\",\"server.tomcat.protocol_header\":\"x-forwarded-proto\",\"ribbon.ConnectTimeout\":\"2000\",\"niws.loadbalancer.default.circuitTripTimeoutFactorSeconds\":\"5\",\"feign.compression.request.min-request-size\":\"2048\",\"duiba.biztool.check.version.biztool\":\"1.5.1\",\"ribbon.NFLoadBalancerPingInterval\":\"3\",\"spring.sleuth.sampler.percentage\":\"1\",\"eureka.client.service-url.defaultZone\":\"http://eureka.duibatest.com.cn/eureka/\",\"spring.zipkin.base-url\":\"http://zipkin-server/\",\"eureka.client.healthcheck.enabled\":\"true\",\"spring.cloud.config.overrideSystemProperties\":\"false\",\"management.health.redis.enabled\":\"false\",\"duiba.sso.mapping-mode\":\"true\",\"ribbon.NFLoadBalancerMaxTotalPingTime\":\"3\",\"ribbon.ReadTimeout\":\"5000\",\"hystrix.command.default.execution.isolation.thread.timeoutInMilliseconds\":\"7150\",\"feign.httpclient.enabled\":\"true\",\"eureka.instance.prefer-ip-address\":\"true\",\"duiba.cloud.etcd.uris\":\"http://etcd.duibatest.com.cn:2379\",\"feign.compression.request.enabled\":\"true\",\"duiba.biztool.check.version.duibaext\":\"1.2.242\"}}]}";
//        String response = HttpClientUtil.httpGet(propUrl, new HashMap<>());
        JSONObject jsonObject = EtlUtil.parseJson(response);
        JSONArray jsonArray = JSON.parseArray(EtlUtil.getString(jsonObject, "propertySources"));
        if (jsonArray != null) {
            for (Object o : jsonArray) {
                JSONObject json = EtlUtil.parseJson(o);
                String name = EtlUtil.getString(json, "name");
                if (StringUtils.isNotBlank(name)&& name.startsWith("apollo")) {
                    props = EtlUtil.parseJson(json.get("source"));
                    break;
                }
            }
        }
    }

    public static PropertyUtil getInstance(String environment) {
        if (instance == null) {
            init(environment);
        }
        return instance;
    }

    private synchronized static void init(String environment) {
        if (instance == null) {
            instance = new PropertyUtil(environment);
        }
    }

    public String getProperty(String key) {

        return props.getString(key);
    }

    public int getPropertyForInt(String key) {
        return props.getIntValue(key);
    }

    public long getPropertyForLong(String key) {
        return props.getLongValue(key);
    }

    public boolean getPropertyForBoolean(String key) {
        return props.getBooleanValue(key);
    }
}
