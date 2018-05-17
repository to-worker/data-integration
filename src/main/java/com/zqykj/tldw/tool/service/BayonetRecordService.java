package com.zqykj.tldw.tool.service;

import com.netposa.recognize.model.ProviderVehicleInfo;
import com.zqykj.tldw.tool.mapper.BayonetRecordMapper;
import com.zqykj.tldw.tool.util.DataSourceUtils;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author feng.wei
 * @date 2018/5/16
 */
public class BayonetRecordService {

    private static Logger logger = LoggerFactory.getLogger(BayonetRecordService.class);

    public List<ProviderVehicleInfo> getAll() {
        SqlSession session = DataSourceUtils.getSession();
        try {
            BayonetRecordMapper mapper = session.getMapper(BayonetRecordMapper.class);
            List<ProviderVehicleInfo> vehicleInfos = mapper.selectAll();
            logger.info("get {} size from database.", vehicleInfos.size());
            return vehicleInfos;
        } finally {
            session.close();
        }

    }

}
