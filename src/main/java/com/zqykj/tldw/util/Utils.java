package com.zqykj.tldw.util;

import com.zqykj.hyjj.entity.elp.Property;
import com.zqykj.tldw.common.JobConstants;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class Utils {

    private static Logger logger = LoggerFactory.getLogger(Utils.class);

    public static final String LIST_SEPERATOR = ",";

    public static final String DEFAULT_DATE_PATTERN = "yyyy-MM-dd";
    public static final String DEFAULT_DATETIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
    public static final String DEFAULT_TIME_PATTERN = "HH:mm:ss";

    private static ThreadLocal<ConcurrentHashMap<String, SimpleDateFormat>> threadLocal = new ThreadLocal<ConcurrentHashMap<String, SimpleDateFormat>>();

    public static SimpleDateFormat getDateFormat(String pattern) {
        ConcurrentHashMap<String, SimpleDateFormat> concurentmap = threadLocal.get();
        if (null == concurentmap) {
            concurentmap = new ConcurrentHashMap<String, SimpleDateFormat>();
            threadLocal.set(concurentmap);
        }
        SimpleDateFormat sdf = threadLocal.get().get(pattern);
        if (null == sdf) {
            sdf = new SimpleDateFormat(pattern);
            threadLocal.get().put(pattern, sdf);
        }
        return sdf;
    }

    public static Date parse(String dateStr, String pattern) throws ParseException {
        if (StringUtils.isBlank(dateStr)) {
            return null;
        }
        return getDateFormat(pattern).parse(dateStr);
    }

    public static String format(Date date, String pattern) {
        if (null == date) {
            return null;
        }
        return getDateFormat(pattern).format(date);
    }

    public static String format(String dateStr, String fromPattern, String toPattern) throws ParseException {
        Date date = parse(dateStr, fromPattern);
        return format(date, toPattern);
    }

    /**
     * What about list with single element? TODO
     * 
     * @param s
     * @return
     */
    public static boolean isValueList(String s) {
        if (null == s) {
            return false;
        }
        return s.indexOf(LIST_SEPERATOR) > 0;
    }

    public static List<Integer> parseIntList(String str) {
        List<Integer> ret = new ArrayList<Integer>();
        if (null == str) {
            return ret;
        }
        String[] intStrArray = str.split(LIST_SEPERATOR);
        for (String intStr : intStrArray) {
            String trimmed = intStr.trim();
            if (trimmed.length() > 0) {
                ret.add(Integer.parseInt(trimmed));
            }
        }
        return ret;
    }

    public static List<Long> parseLongList(String str) {
        List<Long> ret = new ArrayList<Long>();
        if (null == str) {
            return ret;
        }
        String[] intStrArray = str.split(LIST_SEPERATOR);
        for (String intStr : intStrArray) {
            String trimmed = intStr.trim();
            if (trimmed.length() > 0) {
                ret.add(Long.parseLong(trimmed));
            }
        }
        return ret;
    }

    public static List<BigDecimal> parseNumberList(String str) {
        List<BigDecimal> ret = new ArrayList<BigDecimal>();
        if (null == str) {
            return ret;
        }
        String[] strArray = str.split(LIST_SEPERATOR);
        for (String numberStr : strArray) {
            String trimmed = numberStr.trim();
            if (trimmed.length() > 0) {
                ret.add(new BigDecimal(trimmed));
            }
        }
        return ret;
    }

    public static List<Double> parseDoubleList(String str) {
        String[] strArray = str.split(LIST_SEPERATOR);
        List<Double> ret = new ArrayList<Double>();
        for (String numberStr : strArray) {
            String trimmed = numberStr.trim();
            if (trimmed.length() > 0) {
                ret.add(Double.parseDouble(trimmed));
            }
        }
        return ret;
    }

    public static List<String> parseStringList(String str) {
        List<String> ret = new ArrayList<String>();
        if (StringUtils.isBlank(str)) {
            return ret;
        }
        String[] strArray = str.split(LIST_SEPERATOR);
        for (String subStr : strArray) {
            String trimmed = subStr.trim();
            if (trimmed.length() > 0) {
                ret.add(trimmed);
            }
        }
        return ret;
    }

    public static String collectionToString(Collection<String> coll) {
        if (null == coll || coll.isEmpty()) {
            return null;
        }
        StringBuilder sbd = new StringBuilder();
        for (String str : coll) {
            sbd.append(str).append(LIST_SEPERATOR);
        }
        return sbd.substring(0, sbd.length() - 1);
    }

    public static String propToString(Property p, Object o) {
        String value = null;
        if (null == o) {
            return value;
        }
        try {
            switch (p.getType()) {
            case date:
                if (o instanceof Long) {
                    value = Utils.format(new Date((Long) o), Utils.DEFAULT_DATE_PATTERN);
                } else if (o instanceof String) {
                    value = (String) o;
                } else if (o instanceof Date) {
                    value = Utils.format((Date) o, Utils.DEFAULT_DATE_PATTERN);
                }
                break;
            case datetime:
                if (o instanceof Long) {
                    value = Utils.format(new Date((Long) o), Utils.DEFAULT_DATETIME_PATTERN);
                } else if (o instanceof String) {
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(JobConstants.FORMATTER_DATETIME);
                    value = simpleDateFormat.format(simpleDateFormat.parse((String) o));
                } else if (o instanceof Date) {
                    value = Utils.format((Date) o, Utils.DEFAULT_DATETIME_PATTERN);
                }
                break;
            default:
                value = o.toString();
                break;
            }
        } catch (ParseException e) {
            logger.error("parse {} to {} has errir: {}", o, p.getType(), e);
        }

        return value;
    }

    public static Object stringToObject(Property p, String value) {
        switch (p.getType()) {
        case bool: {
            return Boolean.valueOf(value);
        }
        case date: {
            try {
                return Utils.parse(value, Utils.DEFAULT_DATE_PATTERN);
            } catch (ParseException e) {
                return null;
            }
        }
        case datetime: {
            try {
                if (value.length() == 10) {
                    value = value + " 00:00:00";
                }
                return Utils.parse(value, Utils.DEFAULT_DATETIME_PATTERN);
            } catch (ParseException e) {
                return null;
            }
        }
        case time: {
            return value;
        }
        case integer: {
            return Integer.parseInt(value);
        }
        case number: {
            return Double.parseDouble(value);
        }
        case text: {
            return value;
        }
        default:
            throw new RuntimeException("Not supported property type " + p.getType());
        }
    }

}
