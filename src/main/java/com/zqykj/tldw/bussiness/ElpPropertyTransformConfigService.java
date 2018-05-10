package com.zqykj.tldw.bussiness;

import com.mongodb.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class ElpPropertyTransformConfigService implements java.io.Serializable{
	private static final long serialVersionUID = -640214225982178221L;
	private static Logger logger = LoggerFactory.getLogger(ElpPropertyTransformConfigService.class);
	private PropertyTransformService propertyTransformService = null;
    
    public static final String SYSTEMPARAMETER_CODE_ELP_PROPERTY_TRANSFORM = "system.elpmodel.entity.property.transform.supported.datetime.format";
    private static final String SPACK_MARK = "@";
    
    public static final String PROPERTY_TYPE_DATE = "date";
    public static final String PROPERTY_TYPE_DATETIME = "datetime";
    
    private PropertyTransformConfig transformConfig = null;
    private SimpleDateFormat bestFitFormatDate = null;
    private SimpleDateFormat bestFitFormatDatetime = null;
    
    /*<pattern,SimpleDateFormat>*/
	private Set<SimpleDateFormat> recommendFormatList4Date = new HashSet<SimpleDateFormat>();  
	private Set<SimpleDateFormat> recommendFormatList4Datetime = new HashSet<SimpleDateFormat>();
	
    public ElpPropertyTransformConfigService(String mongoIP, int mongoPort, String mongoDB) {
        if (null == mongoIP) {
            logger.error("ElpPropertyTransformConfigService:NULL mongoIP!");
            return;
        }

        propertyTransformService = new PropertyTransformService(mongoIP, mongoPort, mongoDB);
        DBCollection collection = propertyTransformService.getCollection();
        if (null != collection) {
            try {
                DBObject query = new BasicDBObject();
                query.put("code", ElpPropertyTransformConfigService.SYSTEMPARAMETER_CODE_ELP_PROPERTY_TRANSFORM);
                DBObject result = collection.findOne(query);
                if (null != result) {
                    /* find the record via its code */
                    transformConfig = new PropertyTransformConfig();
                    transformConfig.setCode((String) result.get("code"));
                    transformConfig.setName((String) result.get("name"));
                    transformConfig.setValue((Map<String, String>) result.get("value"));
                    transformConfig.makeDateFormatGroups();
                }
            } catch (Exception e) {
            	logger.error("Get property transform config failed!",e);
            }
    	}else {
    		logger.error("collection is null. Cannot connect to mongodb.");
    	}
    }

    private Date parseDateStringViaRecommendation(String value, Set<SimpleDateFormat> set) {
    	Date result = null;
    	if( null == value ) { return null; }
    	
    	Iterator<SimpleDateFormat> ite = set.iterator();
    	while( ite.hasNext() ){
    		SimpleDateFormat sdf = ite.next();
    		try {
				result = sdf.parse(value);
				if( null != result ) {
					return result;
				}
			} catch (ParseException e) {
				// TODO Auto-generated catch block
			}
    	}
    	
    	return result;
    }
    
    private Date parseTypeDatetime(String propertyType, String value) {
    	/*Parse the value in 3 steps:*/
    	Date result = null;
    	/*First:Use bestfit at first*/
    	if( null != bestFitFormatDatetime ) {
    		try {
				result = bestFitFormatDatetime.parse(value);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
			}
    	}
    	
    	/*Second:Use recommendations*/
    	if( null == result ) {
    		result = parseDateStringViaRecommendation(value,recommendFormatList4Datetime);
    	}
    	
    	/*Third: Traverse */
    	if( null == result ) {
    		/*Get pre-defined patterns.*/
        	List<SimpleDateFormat>  list = getPropertyTransformPattern(propertyType);
        	if( null != list ) {
        		for( SimpleDateFormat sdf : list ) {
            		try {
    					result = sdf.parse((String)value);
    					if( null != result ) {
    						bestFitFormatDate = sdf;  /*latest success fit one*/
    						recommendFormatList4Datetime.add(sdf);
    						return result;
    					}
    				} catch (ParseException e) {
    					//logger.info("Error when parsing date-string for PropertyType.date,using pattern=["+sdf.toPattern()+"]. value=["+(String)value+"].");
    				}
            	}
        	}else {
        		logger.info("No patterns found for [date]! Please check your config.MongoDB,collection=SystemParameter; code=system.elpmodel.entity.property.transform.supported.datetime.format");
        	}
    	}
    	
    	return result;
    }
    
    
    private Date parseTypeDate(String propertyType, String value) {
    	/*Parse the value in 3 steps:*/
    	Date result = null;
    	/*First:Use bestfit at first*/
    	if( null != bestFitFormatDate ) {
    		try {
				result = bestFitFormatDate.parse(value);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
			}
    	}
    	
    	/*Second:Use recommendations*/
    	if( null == result ) {
    		result = parseDateStringViaRecommendation(value,recommendFormatList4Date);
    	}
    	
    	/*Third: Traverse */
    	if( null == result ) {
    		/*Get pre-defined patterns.*/
        	List<SimpleDateFormat>  list = getPropertyTransformPattern(propertyType);
        	if( null != list ) {
        		for( SimpleDateFormat sdf : list ) {
            		try {
    					result = sdf.parse((String)value);
    					if( null != result ) {
    						bestFitFormatDate = sdf;  /*latest success fit one*/
    						recommendFormatList4Date.add(sdf);
    						return result;
    					}
    				} catch (ParseException e) {
    					//logger.info("Error when parsing date-string for PropertyType.date,using pattern=["+sdf.toPattern()+"]. value=["+(String)value+"].");
    				}
            	}
        	}else {
        		logger.info("No patterns found for [date]! Please check your config.MongoDB,collection=SystemParameter; code=system.elpmodel.entity.property.transform.supported.datetime.format");
        	}
    	}
    	
    	return result;
    }
    
    /**
     * @param propertyType  	type of a property in an entity/link of an elp model.
     * @param value    			value to parse(Extracted from avro file)
     * @return  Date / null object
     */
    public Date parseDateString(String propertyType, String value) {
    	if( null == propertyType || null == value ) { return null; }
    	
    	if( propertyType.equalsIgnoreCase(ElpPropertyTransformConfigService.PROPERTY_TYPE_DATE) ){
    		return parseTypeDate(propertyType, value);
    	}else if( propertyType.equalsIgnoreCase(ElpPropertyTransformConfigService.PROPERTY_TYPE_DATETIME) ) {
    		return parseTypeDatetime(propertyType, value);
    	}else {
    		logger.info("Unsupported propertyType = "+propertyType);
    	}
    	
    	return null;
    }
    
    public List<SimpleDateFormat> getPropertyTransformPattern(String propertyType) {
        if (null != transformConfig) {
            if( propertyType.equalsIgnoreCase(ElpPropertyTransformConfigService.PROPERTY_TYPE_DATE) ) {
            	return transformConfig.getDateFmts();
            }else if( propertyType.equalsIgnoreCase(ElpPropertyTransformConfigService.PROPERTY_TYPE_DATETIME)  ) {
            	return transformConfig.getDatetimeFmts();
            }else {
            	logger.info("Not supported propertyType yet! propertyType="+propertyType);
            }
        }

        logger.info("No patterns return.");
        return null;
    }
        
    private class PropertyTransformService implements java.io.Serializable{
    	
		private static final long serialVersionUID = 2184271050754053269L;
		private String mongoIP = null;
    	private int mongoPort = 0;
    	private String mongoDB = null;

    	private final String MONGO_COLLECTION_SYSTEMPARAMETER = "SystemParameter";
    	
    	public PropertyTransformService(String mongoIP, int mongoPort, String mongoDB) {
    		this.mongoIP = mongoIP;
    		this.mongoPort = mongoPort;
    		this.mongoDB = mongoDB;
    	}
    	
    	public DBCollection getCollection() {
    		MongoClient mongoClient = null;
			try {
				mongoClient = new MongoClient(mongoIP, mongoPort);
				DB database = mongoClient.getDB(mongoDB);
	            return database.getCollection(MONGO_COLLECTION_SYSTEMPARAMETER);
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				logger.error("Get mongo collection failed!IP="+mongoIP+",port="+mongoPort+",collection="+MONGO_COLLECTION_SYSTEMPARAMETER,e);
			}
			
			return null;
        }
    }
    	
    private class PropertyTransformConfig implements java.io.Serializable{
    	
		private static final long serialVersionUID = -7531204883073479459L;
		//private String id;
        private String code;
        private String name;
        private Map<String, String> value;

        private List<SimpleDateFormat> dateFmts = new ArrayList<SimpleDateFormat>(40);
        private List<SimpleDateFormat> datetimeFmts = new ArrayList<SimpleDateFormat>(20);
        
        public String getCode() {
            return code;
        }

        public void setCode(String code) {
            this.code = code;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Map<String, String> getValue() {
            return value;
        }

        public void setValue(Map<String, String> value) {
            this.value = value;
        }
        
        public List<SimpleDateFormat> getDateFmts() {
        	return dateFmts;
        }
        
        public List<SimpleDateFormat> getDatetimeFmts() {
        	return datetimeFmts;
        }
        
        public void makeDateFormatGroups() {
        	Map<String, String> values = transformConfig.getValue();
        	if( null != values ){
        		String datePatterns = values.get((String)ElpPropertyTransformConfigService.PROPERTY_TYPE_DATE);
        		if( null != datePatterns && !StringUtils.isBlank(datePatterns) ) {
        			/*spilt the patterns string and make SimpleDateFormat groups*/
        			// multiple patterns can be separated by '@'
        			String[] datePatternsArray = datePatterns.split(SPACK_MARK);
                    for( String str:datePatternsArray ){
                    	dateFmts.add(new SimpleDateFormat(str,Locale.ENGLISH));
                    }  
        		}else {
        			logger.info("patterns are blank,propertyType=" + ElpPropertyTransformConfigService.PROPERTY_TYPE_DATE);
        		}
        		
        		String dateTimePatterns = values.get((String)ElpPropertyTransformConfigService.PROPERTY_TYPE_DATETIME);
        		if( null != dateTimePatterns && !StringUtils.isBlank(dateTimePatterns) ) {
        			String[] datetimePatternsArray = dateTimePatterns.split(SPACK_MARK);
                    for( String str:datetimePatternsArray ){
                    	datetimeFmts.add(new SimpleDateFormat(str,Locale.ENGLISH));
                    }  
        		}else {
        			logger.info("patterns are blank,propertyType=" + ElpPropertyTransformConfigService.PROPERTY_TYPE_DATETIME);
        		}
        	}
        }
    }
}
