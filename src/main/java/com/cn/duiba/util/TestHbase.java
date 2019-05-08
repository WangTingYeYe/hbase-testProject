package com.cn.duiba.util;

import com.alibaba.fastjson.JSON;
import com.cn.duiba.util.entitys.OrientationPackageAdjustDiDO;
import com.cn.duiba.util.utils.HBaseResultCreater;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Test;

import java.util.*;

public class TestHbase {

    @Test
    public void raightSql3() {
        HbaseUtil hbaseUtil = HbaseUtil.getInstance("test", "hadoop");

        /**
         * key='advertId_appId_baseType_subType_date' 媒体维度
         * key='advertId_baseType_subType_date' 大盘维度
         * rowkey=key的md5加密前4位+'-'+key
         */
        String advertId = "37530";
        String appId = "57146";
        String baseType = "0";
        String subType = "9";
        String date = "20190505";

        String second_type = "10";
        String adjust_ratio = "0.267";
        String base_pv = "14";
        String b_cvr = "0.0857";
        String data_type = "1";



        String appKey = EtlUtil.getMD5HbaseRowkey(advertId, appId, baseType, subType, date);
        String advertKey = EtlUtil.getMD5HbaseRowkey(advertId, baseType, subType, date);

        Map<String, String> map = new HashMap<>();
        map.put("second_type",second_type);
        map.put("adjust_ratio",adjust_ratio);
        map.put("base_pv",base_pv);
        map.put("b_cvr",b_cvr);
        map.put("advert_id",advertId);
        map.put("base_type",baseType);
        map.put("sub_type",subType);
        map.put("data_type",data_type);
        Map<String, Map<String, String>> rowKeyMap = new HashMap<>();
        rowKeyMap.put(appKey, map);
        rowKeyMap.put(advertKey, map);

        hbaseUtil.insert("tuia_orientation_package_app_backend_adjust_di",rowKeyMap,"cf");
    }

    @Test
    public void slect(){
        HbaseUtil hbaseUtil = HbaseUtil.getInstance("test", "hadoop");
        String advertId = "37530";
        String appId = "57146";
        String baseType = "0";
        String subType = "9";
        String date = "20190505";

        String second_type = "10";
        String adjust_ratio = "0.267";
        String base_pv = "14";
        String b_cvr = "0.0857";
        String data_type = "1";



        String appKey = EtlUtil.getMD5HbaseRowkey(advertId, appId, baseType, subType, date);
        String advertKey = EtlUtil.getMD5HbaseRowkey(advertId, baseType, subType, date);
        Result[] rowList = hbaseUtil.getRowList("tuia_orientation_package_app_backend_adjust_di", Arrays.asList(appKey, advertKey), "cf");

        for (Result result : rowList) {
            Optional<OrientationPackageAdjustDiDO> optionalDo = HBaseResultCreater.of(result, OrientationPackageAdjustDiDO.class).build();
            optionalDo.ifPresent(orientationPackageAdjustDiDO ->  {
                System.out.println(JSON.toJSONString(orientationPackageAdjustDiDO));
            });
        }
    }

    @Test
    public void trustship(){
        HbaseUtil hbaseUtil = HbaseUtil.getInstance("test", "hadoop");

        //广告id 集合
        List<String> advertIds = Arrays.asList("4262");
        //配置id 集合
        List<String> orientIds = Arrays.asList("0","3795");
        //媒体id 集合
        List<String> appIds = Arrays.asList("19003","19002");
        //广告位id 集合
//        List<String> slotIds = Arrays.asList("258480","188712");
        List<String> slotIds = Arrays.asList("258481");
        //日期集合
//        List<String> dates = Arrays.asList("20190507","20190506","20190504","20190503","20190502","20190501","20190430");

//        List<String> dates = Arrays.asList("20190508","20190506","20190504","20190503","20190502","20190501","20190430");
        List<String> dates = Arrays.asList("20190507");


        //小时集合
        List<String> hours = Arrays.asList("00","01","02","03","04","05","06","07","08","09","10","11","12","13","14","15","16","17","18","19","20","21","22","23");

        //维稳因子
        String adjustPriceFactor = "0.8";

        //发券量
        String launchCnt = "400";
        //预估CTR总和
        String totalPreCTR = "40.234";
        //出价Fee总和
        String  totalFee = "8000";
        //消耗
        String consume = "400";
        //底价消耗
        String minConsume = "400";
        //计费点击量
        String clickCnt = "20";
        //落地页转化量
        String effectCnt0 = "6";
        //激活转化量
        String effectCnt2 = "6";
        //注册转化量
        String effectCnt3 = "6";
        //竞价次数
        String bidCnt = "1000";

        Map<String, Map<String, String>> rowKeyMap = new HashMap<>();
        for (String date : dates) {
            for (String hour : hours) {
                String time = date + hour;
                for (String advertId : advertIds) {
                    for (String orientId : orientIds) {
                        //配置维度
                        String orientKey = EtlUtil.getMD5HbaseRowkey("0", advertId, "1", orientId, time);
                        Map<String, String> map = new HashMap<>();
                        map.put("consume",consume);
                        map.put("effectCnt0",effectCnt0);
                        map.put("effectCnt2",effectCnt2);
                        map.put("effectCnt3",effectCnt3);
                        map.put("minConsume",minConsume);
                        map.put("totalFee",totalFee);
                        map.put("launchCnt",launchCnt);
                        map.put("adjustPriceFactor",adjustPriceFactor);
                        map.put("bidCnt",bidCnt);
                        rowKeyMap.put(orientKey, map);

                        //配置 媒体 维度
                        for (String appId : appIds) {
                            String appKey = EtlUtil.getMD5HbaseRowkey("0", advertId, "1", orientId,"3",appId, time);
                            Map<String, String> appMap = new HashMap<>();
                            appMap.put("consume",consume);
                            appMap.put("effectCnt0",effectCnt0);
                            appMap.put("effectCnt2",effectCnt2);
                            appMap.put("effectCnt3",effectCnt3);
                            appMap.put("adjustPriceFactor",adjustPriceFactor);
                            rowKeyMap.put(appKey, appMap);
                        }

                        //配置广告位维度
                        for (String slotId : slotIds) {
                            String appKey = EtlUtil.getMD5HbaseRowkey("0", advertId, "1", orientId,"2",slotId, time);
                            Map<String, String> appMap = new HashMap<>();
                            appMap.put("consume",consume);
                            appMap.put("clickCnt",clickCnt);
                            appMap.put("totalFee",totalFee);
                            appMap.put("totalPreCTR",totalPreCTR);
                            appMap.put("launchCnt",launchCnt);
                            rowKeyMap.put(appKey, appMap);
                        }


                    }

                }
            }
        }

        hbaseUtil.insert("tb_trusteeship_selfsave_di",rowKeyMap,"cf");



    }
    @Test
    public void testKey() {
        //广告id 集合
        String advertId = "4262";
        //配置id 集合
        String orientId = "123";
        //媒体id 集合
        String appId= "57146";
        //广告位id 集合
        String slotId = "57146";
        String time = "2019050723";
        //配置维度
        String orientKey = EtlUtil.getMD5HbaseRowkey("0", advertId, "1", orientId, time);
        //配置媒体维度
        String appKey = EtlUtil.getMD5HbaseRowkey("0", advertId, "1", orientId,"3",appId, time);
        //配置广告位维度
        String slotKey = EtlUtil.getMD5HbaseRowkey("0", advertId, "1", orientId,"2",slotId, time);

        HbaseUtil hbaseUtil = HbaseUtil.getInstance("test", "hadoop");

        hbaseUtil.insert("tb_trusteeship_selfsave_di",orientKey,"cf","column","value");
    }

}
