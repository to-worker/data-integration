package com.zqykj.tldw.service;

import com.zqykj.hyjj.entity.elp.ElpModel;
import com.zqykj.hyjj.entity.elp.Entity;
import com.zqykj.hyjj.entity.elp.Link;
import com.zqykj.hyjj.entity.elp.Property;
import com.zqykj.tldw.dao.ElpModelDao;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Created by weifeng on 2018/5/10.
 */
@Service
public class ElpModelService {

    private static Logger logger = LoggerFactory.getLogger(ElpModelService.class);

    @Autowired
    private ElpModelDao elpModelDao;

    public ElpModel getElpModelByModelId(String modelId) {
        return elpModelDao.findByModelId(modelId);
    }

    public ElpModel findElpByPropOfEntity(String propuuid) {
        Sort sort = new Sort(Sort.Direction.DESC, "version");
        return elpModelDao.findByEntityProp(propuuid, sort);
    }

    public ElpModel findElpByLinkUuid(String linkUuid) {
        Sort sort = new Sort(Sort.Direction.DESC, "version");
        return elpModelDao.findByLinkUUid(linkUuid, sort);
    }

    public ElpModel findElpByEntityUuid(String entityUuid) {
        Sort sort = new Sort(Sort.Direction.DESC, "version");
        return elpModelDao.findByEntityUUid(entityUuid, sort);
    }

    public Entity findEntityByEntityUuid(String entityUuid) {
        ElpModel model = findElpByEntityUuid(entityUuid);
        if (null == model) {
            return null;
        }
        List<Entity> entities = model.getEntities();
        if (CollectionUtils.isEmpty(entities)) {
            return null;
        }
        for (Entity entity : entities) {
            if (null != entity.getUuid()) {
                if (entity.getUuid().equals(entityUuid)) {
                    return entity;
                }
            }
        }
        return null;
    }

    public Link findLinkByLinkUuid(String linkUuid) {
        ElpModel model = findElpByLinkUuid(linkUuid);
        if (null == model) {
            return null;
        }
        List<Link> links = model.getLinks();
        if (CollectionUtils.isEmpty(links)) {
            return null;
        }
        for (Link link : links) {
            if (null != link.getUuid()) {
                if (link.getUuid().equals(linkUuid)) {
                    return link;
                }
            }
        }
        return null;
    }

}
