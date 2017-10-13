/* Copyright(c) 2017 AsiaInfo Technologies (China), Inc.
 * All Rights Reserved.
 * FileName: CommonServiceImpl.java
 * Author:   <a href="mailto:xiongjie3@asiainfo.com">xiongjie3</a>
 * Date:     2017年2月28日 下午4:19:40
 * Description: DDL操作     
 */
package com.ai.tag.service.impl;

import com.ai.tag.common.StringConstant;
import com.ai.tag.common.TagException;
import com.ai.tag.dao.IBaseDao;
import com.ai.tag.service.CommonService;

import javax.annotation.Resource;

import org.springframework.stereotype.Service;

/**
 * DDL操作<br>
 *
 * @author xiongjie3
 */
@Service("commonService")
public class CommonServiceImpl implements CommonService {

	@Resource(name = "iBaseDao")
	private IBaseDao baseDao;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ai.tag.service.CommonService#isTableExists(java.lang.String)
	 */
	@Override
	public boolean isTableExists(String tableName) throws TagException {

		String[] tableNames = tableName.split("\\.");

		String table = tableNames.length == 2 ? tableNames[1] : tableNames[0];
		String schema = tableNames.length == 2 ? tableNames[0] : StringConstant.SCHEMA_DW;

		return baseDao.doesTableExist(schema, table);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ai.tag.service.CommonService#isIndexExists(java.lang.String)
	 */
	@Override
	public boolean isIndexExists(String indexName) throws TagException {

		String[] indexNames = indexName.split("\\.");

		String index = indexNames.length == 2 ? indexNames[1] : indexNames[0];
		String schema = indexNames.length == 2 ? indexNames[0] : StringConstant.SCHEMA_DW;

		return baseDao.doesIndexExist(schema, index);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ai.tag.service.CommonService#dropTable(java.lang.String)
	 */
	@Override
	public void dropTable(String tableName) throws TagException {
		baseDao.dropTable(tableName);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ai.tag.service.CommonService#truncateTable(java.lang.String)
	 */
	@Override
	public void truncateTable(String tableName) throws TagException {
		baseDao.truncateTable(tableName);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ai.tag.service.CommonService#createTempTable(java.lang.String,
	 * java.lang.String, java.lang.String, java.lang.String, java.lang.String,
	 * java.lang.String, boolean)
	 */
	@Override
	public void createTempTable(String tableName, String fields, String tableSpace, String indexTabs,
			String partitionKey, String distKey, int flag) throws TagException {
		// TODO Auto-generated method stub

		if (isTableExists(tableName)) {

			if (1 == flag) {// 要创建的目标表已经存在,删除已经它，然后再新建
				// 先清空数据
				truncateTable(tableName);
				// 再删除表
				dropTable(tableName);
			}

		}

		createTable(tableName, fields, tableSpace, indexTabs, partitionKey, distKey);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ai.tag.service.CommonService#createTable(java.lang.String,
	 * java.lang.String, java.lang.String, java.lang.String, java.lang.String,
	 * java.lang.String)
	 */
	@Override
	public void createTable(String tableName, String fields, String tableSpace, String tabsIndex, String partitionKey,
			String distribute) throws TagException {

		baseDao.createTable(tableName, fields, tableSpace, distribute, tabsIndex, partitionKey);

	}

}
