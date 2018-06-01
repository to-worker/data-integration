package com.zqykj.tldw.kafka;

import com.netposa.recognize.model.ProviderVehicleInfo;
import com.zqykj.tldw.common.JobConstants;
import com.zqykj.tldw.service.Producer;
import com.zqykj.tldw.util.BeanUtils;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

/**
 * @author feng.wei
 * @date 2018/5/9
 */
public class KafkaTest {

    @Test
    public void testSend() {
        String topic = "foshan_test";
        Producer<byte[]> producer = new Producer<byte[]>(topic);
        int i = 0;
        while (i < 40) {
            i++;
            ProviderVehicleInfo bean = new ProviderVehicleInfo();
            bean.setKkbh(UUID.randomUUID().toString().substring(0,8));
            // "苏A68UF" + UUID.randomUUID().toString().substring(0,2)
            bean.setHphm("苏A68UF" + UUID.randomUUID().toString().substring(0,2));
            bean.setCameraId("001");
            bean.setCdbh("车道01");
            bean.setCdfx("由西向东");
            // 车辆类型
            bean.setCllx("jiaoche");
            // 车辆速度
            bean.setClsd(90);
            bean.setJgsj(new Date().getTime() + "");

            bean.setTzsj("test".getBytes());
            bean.setHasExtractFeature(1);
            bean.setIodImageDown(true);
            producer.send(BeanUtils.toByteArray(bean));
//            try {
//                Thread.sleep(0);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
        }
    }

    @Test
    public void testDateFormat(){
        String value = "2018-05-24 13:50:28.0";
        Date date = null;
        try {
            date = new SimpleDateFormat(JobConstants.FORMATTER_DATETIME).parse(value.toString());
            System.out.println(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
