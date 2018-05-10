package com.zqykj.tldw.dao;

import com.zqykj.hyjj.entity.elp.ElpModel;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;

/**
 * Created by weifeng on 2018/5/10.
 */
public interface ElpModelDao extends MongoRepository<ElpModel, String> {

    ElpModel findByModelId(String modelId);

    @Query("{'entities':{'$elemMatch':{'properties':{'$elemMatch':{'uuid':?0}}}}}")
    ElpModel findByEntityProp(String propuuid, Sort sort);

    @Query("{'links':{'$elemMatch':{'properties':{'$elemMatch':{'uuid':?0}}}}}")
    ElpModel findByLinkProp(String propuuid, Sort sort);

}
