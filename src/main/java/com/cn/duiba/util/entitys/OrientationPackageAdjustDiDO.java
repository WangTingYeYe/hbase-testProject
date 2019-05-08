package com.cn.duiba.util.entitys;

import java.util.Objects;

/**
 * @author wangting
 * @version 1.0
 * @ClassName: OrientationPackageAdjustDiDO
 * Function: Hbase 深度优化数据实体
 * Date:     2019/4/25 0025 上午 11:38
 */
public class OrientationPackageAdjustDiDO {

    /**
     * 广告
     */
    private Long advertId;

    /**
     * 媒体ID
     */
    private Long appId;


    /**
     * 投放目标类型
     */
    private Long baseType;

    /**
     * 辅助目标类型
     */
    private Long secondSubType;
    /**
     * 后端转化率
     */
    private Double bCvr;

    /**
     * 前端pv
     */
    private Long basePv;

    public OrientationPackageAdjustDiDO() {
    }

    private OrientationPackageAdjustDiDO(Builder builder) {
        setAdvertId(builder.advertId);
        setAppId(builder.appId);
        setBaseType(builder.baseType);
        setSecondSubType(builder.secondSubType);
        setbCvr(builder.adjustRatio);
        setBasePv(builder.basePv);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(OrientationPackageAdjustDiDO copy) {
        Builder builder = new Builder();
        builder.advertId = copy.getAdvertId();
        builder.appId = copy.getAppId();
        builder.baseType = copy.getBaseType();
        builder.secondSubType = copy.getSecondSubType();
        builder.adjustRatio = copy.getbCvr();
        builder.basePv = copy.getBasePv();
        return builder;
    }


    @Override
    public int hashCode() {
        return Objects.hash(getAdvertId(), getAppId(), getBaseType(), getSecondSubType());
    }

    public Long getAdvertId() {
        return advertId;
    }

    public void setAdvertId(Long advertId) {
        this.advertId = advertId;
    }

    public Long getAppId() {
        return appId;
    }

    public void setAppId(Long appId) {
        this.appId = appId;
    }

    public Long getBaseType() {
        return baseType;
    }

    public void setBaseType(Long baseType) {
        this.baseType = baseType;
    }

    public Long getSecondSubType() {
        return secondSubType;
    }

    public void setSecondSubType(Long secondSubType) {
        this.secondSubType = secondSubType;
    }

    public Double getbCvr() {
        return bCvr;
    }

    public void setbCvr(Double bCvr) {
        this.bCvr = bCvr;
    }

    public Long getBasePv() {
        return basePv;
    }

    public void setBasePv(Long basePv) {
        this.basePv = basePv;
    }


    public static final class Builder {
        private Long advertId;
        private Long appId;
        private Long baseType;
        private Long secondSubType;
        private Double adjustRatio;
        private Long basePv;

        private Builder() {
        }

        public Builder advertId(Long val) {
            advertId = val;
            return this;
        }

        public Builder appId(Long val) {
            appId = val;
            return this;
        }

        public Builder baseType(Long val) {
            baseType = val;
            return this;
        }

        public Builder secondSubType(Long val) {
            secondSubType = val;
            return this;
        }

        public Builder adjustRatio(Double val) {
            adjustRatio = val;
            return this;
        }

        public Builder basePv(Long val) {
            basePv = val;
            return this;
        }

        public OrientationPackageAdjustDiDO build() {
            return new OrientationPackageAdjustDiDO(this);
        }
    }
}
