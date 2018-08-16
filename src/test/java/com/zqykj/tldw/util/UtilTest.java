package com.zqykj.tldw.util;

import com.netposa.recognize.model.ProviderVehicleInfo;
import org.junit.Test;

import java.util.Map;

/**
 * @author feng.wei
 * @date 2018/8/15
 */
public class UtilTest {

    @Test
    public void testBeanGetFields(){
        Map<String, String> map = BeanUtils.getFields(new ProviderVehicleInfo());
        System.out.println(map);
    }
}
