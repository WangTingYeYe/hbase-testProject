package com.cn.duiba.util.utils;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.PropertyAccessorFactory;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author ZhouFeng zhoufeng@duiba.com.cn
 * @version $Id: HBaseResultCreater.java , v 0.1 2017/10/30 下午4:58 ZhouFeng Exp $
 */
public class HBaseResultCreater<T> {

    private final Logger logger = LoggerFactory.getLogger(HBaseResultCreater.class);

    private Result result;
    private Class<? extends T>                                             resultType;

    private static final LoadingCache<Class<?>, Map<String, PropertyDescriptor>> CACHE =
            CacheBuilder.newBuilder().build(new CacheLoader<Class<?>, Map<String, PropertyDescriptor>>() {

                 @Override
                 public Map<String, PropertyDescriptor> load(Class<?> key) throws Exception {
                     return mappingField(key);
                 }
             });

    private HBaseResultCreater(Result result, Class<T> clazz) {
        this.result = result;
        this.resultType = clazz;
    }

    public static <T> HBaseResultCreater<T> of(Result result, Class<T> clazz) {
        return new HBaseResultCreater<>(result, clazz);
    }

    public Optional<T> build() {

        if (result == null || result.isEmpty()) {
            return Optional.empty();
        }

        if (resultType == null) {
            throw new IllegalArgumentException("resultType cannot be null");
        }

        T bean = BeanUtils.instantiate(resultType);

        Map<String, PropertyDescriptor> fieldDescriptorMap;
        try {
            fieldDescriptorMap = CACHE.get(resultType);
        } catch (Exception e) {
            return Optional.empty();
        }

        List<Cell> cells = result.listCells();

        BeanWrapper beanWrapper = PropertyAccessorFactory.forBeanPropertyAccess(bean);

        for (Cell cell : cells) {
            String qualifier = new String(cell.getQualifierArray(), cell.getQualifierOffset(),
                    cell.getQualifierLength());

            PropertyDescriptor propertyDescriptor = fieldDescriptorMap.get(qualifier);
            if (propertyDescriptor == null) {
                continue;
            }

            Object value = null;
            if(propertyDescriptor.getPropertyType().equals(Long.class)){
                value = new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            }else if(propertyDescriptor.getPropertyType().equals(String.class)){
                value = new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            }else if(propertyDescriptor.getPropertyType().equals(Double.class)){
                String temp = new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                value = Double.valueOf(temp);
            }

            if(value != null){
                beanWrapper.setPropertyValue(propertyDescriptor.getName(), value);
            }

        }

        return Optional.of(bean);

    }

    /**
     * 遍历对象中的所有字段，建立字段名与PropertyDescriptor的映射
     */
    public static Map<String, PropertyDescriptor> mappingField(Class<?> resultType) {

        Map<String, PropertyDescriptor> fieldDescriptorMap;

        PropertyDescriptor[] propertyDescriptors = BeanUtils.getPropertyDescriptors(resultType);

        fieldDescriptorMap = new HashMap<>(propertyDescriptors.length);

        for (PropertyDescriptor descriptor : propertyDescriptors) {

            String fieldName = descriptor.getName();

            fieldDescriptorMap.put(fieldName, descriptor);

            fieldDescriptorMap.put(camelCase2SnakeCase(fieldName), descriptor);

            // 判断是否使用了别名
            Field field = FieldUtils.getField(resultType, fieldName, true);
            if (field != null) {
                HBaseField annotation = field.getAnnotation(HBaseField.class);
                if (annotation != null) {
                    String alias = annotation.alias();
                    if (StringUtils.isNotBlank(alias)) {
                        fieldDescriptorMap.put(alias, descriptor);
                    }

                }
            }

        }
        return fieldDescriptorMap;
    }

    /**
     * 驼峰命名法(camel case)字段名转为下划线命名法(snake case)字段名
     *
     * @param camelCaseName
     * @return
     */
    private static String camelCase2SnakeCase(String camelCaseName) {
        char[] chars = camelCaseName.toCharArray();
        StringBuilder sb = new StringBuilder();

        for (char aChar : chars) {
            // 如果当前字母为大写，则在其前面添加一个下划线，并将其转为小写
            if (Character.isUpperCase(aChar)) {
                sb.append("_");
                aChar = Character.toLowerCase(aChar);
            }

            sb.append(aChar);
        }

        return sb.toString();

    }

}
