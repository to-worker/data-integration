package com.zqykj.tldw.dao;

import com.zqykj.hyjj.entity.elp.ElpModelDBMapping;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;

import java.util.List;

/**
 * @author feng.wei
 * @date 2018/5/10
 */
public interface ElpDBMappingDao extends MongoRepository<ElpModelDBMapping, String> {

    @Query("{'tableName':?0,'ds':?1,'elp':?2}")
    List<ElpModelDBMapping> findByTableNameAndDsAndElp(String tableName, String ds, String elp);

    @Query("{'ds':?0,'tableName':?1,'elp':?2,'elpType':?3}")
    ElpModelDBMapping findOneByTableNameAndElpType(String ds, String tableName, String elp, String elpType);

}
