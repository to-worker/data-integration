db.getCollection('ElpModelDBMapping').insert([
    {
        "_class" : "com.zqykj.hyjj.entity.elp.ElpModelDBMapping",
        "uuid" : "foshan-bayonet-pass-record-uuid-test",
        "name" : "foshan-kafka",
        "ds" : "kafka",
        "tableName" : "bayonet_pass_record",
        "elp" : "foshan_standard_model",
        "elpType" : "bayonet_pass_record",
        "elpTypeDesc" : "Link",
        "directivity" : "SourceToTarget",
        "directionColumn" : {},
        "condition" : {
            "type" : "and",
            "colFilters" : []
        },
        "dbmap" : [
            {
                "propertyUuid" : "pass_time",
                "propertyName" : "经过时间",
                "columnName" : "jgsj"
            },
            {
                "propertyUuid" : "bpr_bayonet_id",
                "propertyName" : "卡口ID",
                "columnName" : "kkbh"
            },
            {
                "propertyUuid" : "vehicle_number",
                "propertyName" : "车辆号牌",
                "columnName" : "hphm"
            },
            {
                "propertyUuid" : "vehicle_speed",
                "propertyName" : "车辆速度",
                "columnName" : "clsd"
            },
            {
                "propertyUuid" : "bpr_lane_id",
                "propertyName" : "车道ID",
                "columnName" : "cdbh"
            },
            {
                "propertyUuid" : "bpr_lane_direction",
                "propertyName" : "车道方向",
                "columnName" : "cdfx"
            },
            {
                "propertyUuid" : "break_rule_type",
                "propertyName" : "违章类型",
                "columnName" : "wzlx"
            }
        ]
    },
    {
        "_class" : "com.zqykj.hyjj.entity.elp.ElpModelDBMapping",
        "uuid" : "foshan-vehicle-uuid-test",
        "name" : "foshan-kafka",
        "ds" : "kafka",
        "tableName" : "bayonet_pass_record",
        "elp" : "foshan_standard_model",
        "elpType" : "vehicle",
        "elpTypeDesc" : "Entity",
        "directionColumn" : {},
        "condition" : {
            "type" : "and",
            "colFilters" : []
        },
        "dbmap" : [
            {
                "propertyUuid" : "vehicle_id",
                "propertyName" : "车辆ID",
                "columnName" : "hphmId"
            },
            {
                "propertyUuid" : "vehicle_plate_code",
                "propertyName" : "车牌号",
                "columnName" : "hphm"
            },
            {
                "propertyUuid" : "vehicle_owner_ssn_id",
                "propertyName" : "所有人身份证",
                "columnName" : "DJMJSFZH"
            }
        ]
    }

])
