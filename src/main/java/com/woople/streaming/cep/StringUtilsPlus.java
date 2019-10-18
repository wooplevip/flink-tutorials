package com.woople.streaming.cep;

import java.text.SimpleDateFormat;
import java.util.Date;

public class StringUtilsPlus {
    public static String stampToDate(String s){
        return stampToDate(Long.parseLong(s));
    }
    public static String stampToDate(long t){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return simpleDateFormat.format(new Date(t));
    }
}
