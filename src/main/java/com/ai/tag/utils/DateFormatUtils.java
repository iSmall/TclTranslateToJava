/* Copyright(c) 2017 AsiaInfo Technologies (China), Inc.
 * All Rights Reserved.
 * FileName: DateFormatUtils.java
 * Author:   <a href="mailto:xiongjie3@asiainfo.com">xiongjie3</a>
 * Date:     2017年2月23日 下午2:47:35
 * Description: 时间格式转换类
 */
package com.ai.tag.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import com.ai.tag.common.TagConstant;
import com.ai.tag.common.TagException;

/**
 * 时间转换类<br>
 *
 * @author xiongjie3
 */
public class DateFormatUtils {

    /**
     * 功能描述:账期格式化<br>
     * 检查传入的账期参数格式,并根据指标周期类型,格式化传入的账期参数:</br>
     * 日周期:传入的账期格式yyyyMMdd;不用格式化;月周期:传入的账期格式yyyyMM,末尾添加01
     *
     * @param date      账期时间
     * @param cycleType 周期类型
     * @return date
     * @throws ParseException
     */
    public static String opTimeDateFormat(String date, int cycleType) throws ParseException {

        String formatResult = "";

        if (TagConstant.DAY_FLAG == cycleType) {
            strToDate_YYYYMMDD(date);//检查格式
            formatResult = date;
        } else if (TagConstant.MONTH_FLAG == cycleType) {
            formatResult = dateToStr(strToDate_YYYYMM(date), "yyyyMM");//检查格式
        } else {
            throw new TagException("cycleType 统计周期参数只能传入1或者2");
        }
        return formatResult;
    }

    /**
     * 功能描述: 获取当前月份最后一天，格式为yyyyMMdd<br>
     *
     * @param date
     * @return
     */
    public static String getCurrentMonthLastDayOfYYYYMMDD(Date date) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.add(Calendar.MONTH, 1);
        calendar.add(Calendar.DATE, -1);
        Date newDate = calendar.getTime();
        return simpleDateFormat.format(newDate);
    }


    public static String dateToStr_YYYYMMDD(Date date) {
        return dateToStr(date, "yyyyMMdd");
    }

    public static Date strToDate_YYYYMMDD(String date) throws ParseException {
        return strToDate(date, "yyyyMMdd");
    }

    public static String dateToStr_YYYY_MM_DD(Date date) {
        return dateToStr(date, "yyyy-MM-dd");
    }

    public static Date strToDate_YYYY_MM_DD(String date) throws ParseException {
        return strToDate(date, "yyyy-MM-dd");
    }

    public static String dateToStr_YYYYMM(Date date) {
        return dateToStr(date, "yyyyMM");
    }

    public static Date strToDate_YYYYMM(String date) throws ParseException {
        return strToDate(date, "yyyyMM");
    }

    public static String dateToStr_YYYY_MM(Date date) {
        return dateToStr(date, "yyyy-MM");
    }

    public static Date strToDate_YYYY_MM(String date) throws ParseException {
        return strToDate(date, "yyyy-MM");
    }

    public static String getLastDay(String date) throws ParseException {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(strToDate_YYYYMMDD(date));
        calendar.add(Calendar.DAY_OF_MONTH, -1);
        return dateToStr_YYYYMMDD(calendar.getTime());
    }

    public static String getLastMonth_YYYYMM(String date) throws ParseException {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(strToDate_YYYYMMDD(date));
        calendar.add(Calendar.MONTH, -1);
        return dateToStr_YYYYMM(calendar.getTime());
    }


    public static String getDateBySubtractDay(String date, int subtract) throws ParseException {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(strToDate_YYYYMMDD(date));
        calendar.add(Calendar.DAY_OF_MONTH, -subtract);
        return dateToStr_YYYYMMDD(calendar.getTime());
    }

    public static String getDateBySubtractMonth_YYYYMM(String date, int subtract) throws ParseException {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(strToDate_YYYYMMDD(date));
        calendar.add(Calendar.MONTH, -subtract);
        return dateToStr_YYYYMM(calendar.getTime());
    }

    public static String dateToStr(Date date, String fromatStr) {
        if (date == null)
            return null;
        SimpleDateFormat sf = new SimpleDateFormat(fromatStr);
        return sf.format(date);
    }

    public static Date strToDate(String date, String formatStr) throws ParseException {
        if (date == null)
            return null;
        SimpleDateFormat sf = new SimpleDateFormat(formatStr);
        return sf.parse(date);
    }

    public static void main(String[] args) throws ParseException {
        System.err.println(getDateBySubtractDay("20170304", 31));
    }

}
