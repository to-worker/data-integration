package com.zqykj.tldw.tool.mapper;

import com.netposa.recognize.model.ProviderVehicleInfo;
import com.zqykj.tldw.common.Constants;
import com.zqykj.tldw.common.TldwConfig;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @author feng.wei
 * @date 2018/5/16
 */
public interface BayonetRecordMapper {

    @Select("SELECT * FROM bayonet_pass_record")
    List<ProviderVehicleInfo> selectAll();

}
