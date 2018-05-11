package com.zqykj.tldw.kafka;

import com.netposa.recognize.model.ProviderVehicleInfo;
import com.zqykj.tldw.service.Producer;
import com.zqykj.tldw.util.BeanUtils;
import org.junit.Test;

import java.util.Date;
import java.util.UUID;

/**
 * @author feng.wei
 * @date 2018/5/9
 */
public class KafkaTest {

    @Test
    public void testSend() {
        String topic = "iod_gcjlout";
        Producer<byte[]> producer = new Producer<byte[]>(topic);
        while (true) {
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
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
