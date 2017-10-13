package com.ai.tag.dao;

import com.ai.tag.BaseTagBizTest;

import javax.annotation.Resource;

import org.junit.Test;
import org.springframework.test.annotation.Rollback;
import org.springframework.transaction.annotation.Transactional;

public class LabelDataGenerationDaoTest extends BaseTagBizTest{

    
    @Resource
	private ILabelDataGenerationDao dao;

	

    @Test
    @Transactional
    @Rollback(true)
	public void testUpdateLabelAsRunning() {
		dao.updateLabelAsRunning(1L, 1L, "20170101", "2017-01-01", 1);
	}
	
    @Test
    @Transactional
    @Rollback(true)
	public void testInsertSubLabels(){
		dao.insertSubLabels("20170101", 1L, 1L, "2017-01-01", 1);
	}

    @Test
    @Transactional
    @Rollback(true)
	public void testGetRunningLabelList(){
		dao.getRunningLabelList("20170101", 1L);
	}
	
	@Test
	@Rollback(true)
	@Transactional
	public void testInsertDstStatus(){
		dao.insertDstStatus("TEST-001", "20170101", 1, 1, 1L, 1L);
	}
	
	@Test
	@Rollback(true)
	@Transactional
	public void testInsetDataIntoNewTempTable(){
		dao.insetDataIntoNewTempTable("SCCOC.DIM_COC_INDEX_STATUS", "SCCOC.DIM_COC_INDEX_STATUS", "20170317");
	}
}
