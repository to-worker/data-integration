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

    public ElpModel findElpByPropOfLink(String propuuid) {
        Sort sort = new Sort(Sort.Direction.DESC, "version");
        return elpModelDao.findByLinkProp(propuuid, sort);
    }

    public Entity findEntityByProp(String propuuid) {
        ElpModel model = findElpByPropOfEntity(propuuid);
        if (null == model) {
            return null;
        }
        List<Entity> entities = model.getEntities();
        if (CollectionUtils.isEmpty(entities)) {
            return null;
        }
        for (Entity entity : entities) {
            List<Property> propties = entity.getProperties();
            if (CollectionUtils.isEmpty(propties)) {
                continue;
            }
            for (Property prop : propties) {
                if (propuuid.equals(prop.getUuid())) {
                    return entity;
                }
            }
        }
        return null;
    }


    public Link findLinkByProp(String propuuid) {
        ElpModel model = findElpByPropOfLink(propuuid);
        if (null == model) {
            return null;
        }
        List<Link> links = model.getLinks();
        if (CollectionUtils.isEmpty(links)) {
            return null;
        }
        for (Link link : links) {
            List<Property> propties = link.getProperties();
            if (CollectionUtils.isEmpty(propties)) {
                continue;
            }
            for (Property prop : propties) {
                if (propuuid.equals(prop.getUuid())) {
                    return link;
                }
            }
        }
        return null;
    }

}
