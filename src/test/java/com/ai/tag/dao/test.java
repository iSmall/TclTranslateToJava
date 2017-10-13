package com.ai.tag.dao;

import com.ai.tag.utils.DateFormatUtils;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.text.ParseException;
import java.util.Date;

/**
 * Created by admin on 2017/6/22.
 */

public class test {

    @Autowired
    private IBaseDao baseDao;
    static int b = 2;

    public static void main(String[] a) {
        String strDate = "201705";

        System.out.print(b);
        try {
            System.out.println(DateFormatUtils.dateToStr(DateFormatUtils.strToDate_YYYYMM(strDate), "yyyyMM"));
            Date date = DateFormatUtils.strToDate_YYYYMM(strDate);
            String strDate1 = DateFormatUtils.dateToStr(date, "yyyy-MM");
            System.out.println(date + "!" + strDate1);
            System.out.print(DateFormatUtils.dateToStr_YYYY_MM_DD(DateFormatUtils.strToDate_YYYYMMDD("20170501")));
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test1() {
        String a = "abc";
        int index = a.indexOf(".");
        System.out.print(index + a.substring(0,index));
//        StringBuilder a1 = new StringBuilder("'qwe|'");
//        String a = "'sccoc.dw_coc_index_100'";
//        System.out.println(a + a1.toString());
//        String[] a1 = a.split("\\u002E");
//        System.out.println(a1.length);
//        if(a1.length == 2) {
//            System.out.println(a1[0]);
//        }
//        }else {
//            System.out.println(a1[0]);
//        }
    }
}
