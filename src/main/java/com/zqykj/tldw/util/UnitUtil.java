package com.zqykj.tldw.util;

import org.springframework.util.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author feng.wei
 * @date 2018/8/15
 */
public class UnitUtil {

    public static final String regex = "(\\d+)([GgMmKk])?";

    public static long getSizeByUnit(String maxSizeString) throws Exception {
        String unit = null;
        long number = -1;
        long maxSize = -1;
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(maxSizeString);
        if (matcher.matches()) {
            number = Long.parseLong(matcher.group(1));
            unit = matcher.group(2);

            if (StringUtils.isEmpty(unit)) {
                unit = "g";
            }
        } else {
            throw new Exception("maxSizeString " + maxSizeString + " pattern is error");
        }

        if ("g".equalsIgnoreCase(unit)) {
            maxSize = number * 1024 * 1024 * 1024;
        } else if ("m".equalsIgnoreCase(unit)) {
            maxSize = number * 1024 * 1024;
        } else if ("k".equalsIgnoreCase(unit)) {
            maxSize = number * 1024;
        }
        return maxSize;
    }
}
