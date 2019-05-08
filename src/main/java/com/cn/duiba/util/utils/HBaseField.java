package com.cn.duiba.util.utils;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * duiba.com.cn Inc. Copyright (c) 2014-2017 All Rights Reserved.
 */

@Target({ ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
public @interface HBaseField {

    String alias() default "";

}
