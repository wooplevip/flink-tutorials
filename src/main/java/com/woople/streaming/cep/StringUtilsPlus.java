package com.woople.streaming.cep;

import java.text.ParseException;
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
    public static long dateToStamp(String dataStr){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = null;
        try {
            date = simpleDateFormat.parse(dataStr);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date.getTime();
    }
}
