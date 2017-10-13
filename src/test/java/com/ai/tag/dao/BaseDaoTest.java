package com.ai.tag.dao;

import com.ai.tag.BaseTagBizTest;

import javax.annotation.Resource;

import org.junit.Test;
import org.springframework.test.annotation.Rollback;
import org.springframework.transaction.annotation.Transactional;

import junit.framework.Assert;

public class BaseDaoTest extends BaseTagBizTest{

    @Resource(name = "iBaseDao")
	private IBaseDao dao = null;
	
	private static final String schemaName = "SCCOC";
	private static final String tableName = "JUNIT_TEST_2017";
	
	private static final String indexName = "IDX_JUNIT_TEST_2017";
	
	private static final String reName = "IDX_JUNIT_TEST_2017_R";
	
	
	@Test
	public void testDoesTableExist(){
		boolean result = dao.doesTableExist(schemaName, tableName);
		if(result){
			dao.truncateTable(schemaName + "." + tableName);
			dao.dropTable(schemaName + "." + tableName);
		}
	}
	
	@Test
    @Transactional
    @Rollback(true)
	public void testCreateTable() {
		
		String columnStr = "OP_TIME     DATE,"
				+ "CITY_ID     SMALLINT,"
				+ "BRAND_ID    SMALLINT,"
				+ "VIP_LEVEL_ID SMALLINT";
		String distKey = "BRAND_ID,VIP_LEVEL_ID,CITY_ID";
		dao.createTable(schemaName + "." + tableName, columnStr, null, distKey, null, "0");
	}
	
	@Test
	public void testDoesTableExist2(){
		boolean result = dao.doesTableExist(schemaName, tableName);
		Assert.assertEquals(true, result);
	}

	@Test
    @Transactional
    @Rollback(true)
	public void testRenameTable(){
		this.dao.renameTable(schemaName, tableName, reName);
	}
	
	@Test
    @Transactional
    @Rollback(true)
	public void testCreateIndex(){
		this.dao.createIndex(schemaName + "." + indexName, schemaName + "." + reName, "CITY_ID,BRAND_ID");
	}
	
	
	@Test
    @Transactional
    @Rollback(false)
	public void testDoesIndexExist(){
		boolean result = dao.doesIndexExist(schemaName, indexName);
		Assert.assertEquals(true, result);
	}
	
	@Test
    @Transactional
    @Rollback(true)
	public void testDbDisableLog(){
		this.dao.dbDisableLog(schemaName + "." + reName);
	}
	
	@Test
    @Transactional
    @Rollback(false)
	public void testIsEmptyTable(){
		boolean result = dao.isEmptyTable(schemaName + "." + reName);
		Assert.assertEquals(false, result);
	}
	
	@Test
    @Transactional
    @Rollback(true)
	public void testTruncateTable(){
		this.dao.truncateTable(schemaName + "." + reName);
	}
	
	@Test
    @Transactional
    @Rollback(true)
	public void testDropTable(){
		this.dao.dropTable(schemaName + "." + reName);
	}
	
}
