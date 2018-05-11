package com.zqykj.tldw.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by weifeng on 2018/5/11.
 */
public class DateUtils {

    /**
     * UTC format
     */
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'23:59:00'Z'");

    public static String getUTCTime(Integer days) {
        // 1、取得本地时间：
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DAY_OF_YEAR, days);
        String time = sdf.format(cal.getTime());
        return time;
    }

}
