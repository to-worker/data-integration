package com.zqykj.tldw.service;

import com.zqykj.hyjj.entity.elp.ElpModelDBMapping;
import com.zqykj.tldw.dao.ElpDBMappingDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author feng.wei
 * @date 2018/5/10
 */
@Service
public class ElpDBMappingService {

    private static Logger logger = LoggerFactory.getLogger(ElpDBMappingService.class);

    @Autowired
    private ElpDBMappingDao elpDBMappingDao;

    public ElpModelDBMapping getElpModelDBMappingByElpTypeAndDs(String ds, String tableName, String elp,
            String elpType) {
        logger.info("ds: {}, tableName:{}, elp: {}, elpType: {}", ds, tableName, elp, elpType);
        return elpDBMappingDao.findOneByTableNameAndElpType(ds, tableName, elp, elpType);
    }

    public Map<String, String> getElpColMap(ElpModelDBMapping elpModelDBMapping) {
        Map<String, String> mappings = new HashMap<>();
        List<ElpModelDBMapping.PropertyColumnPair> dbmap = elpModelDBMapping.getDbmap();
        for (ElpModelDBMapping.PropertyColumnPair colMap : dbmap) {
            String columnName = colMap.getColumnName();
            String propertyUuid = colMap.getPropertyUuid();
            mappings.put(columnName, propertyUuid);
            logger.info("elpType: {}, column: {}, propertyUUid: {}", elpModelDBMapping.getElpType(), columnName,
                    propertyUuid);
        }
        return mappings;
    }

}
