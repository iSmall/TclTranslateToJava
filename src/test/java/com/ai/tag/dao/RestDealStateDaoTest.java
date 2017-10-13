package com.ai.tag.dao;

import com.ai.tag.BaseTagBizTest;
import com.ai.tag.common.StatusConstant;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.junit.Test;
import org.springframework.test.annotation.Rollback;
import org.springframework.transaction.annotation.Transactional;

public class RestDealStateDaoTest extends BaseTagBizTest{

    @Resource
    private IRestDealStateDao dao;


    @Test
    @Transactional
    @Rollback(false)
    public void testQueryUnsuccessfulStatus() {
        List<Map<String, Object>> unsuccessFulStatus = dao.queryUnsuccessfulStatus("20161026");
        System.out.println("===============" + unsuccessFulStatus.size());
    }

    @Test
    @Transactional
    @Rollback(true)
    public void testUpdateExceptionTableStatus() {
        dao.updateExceptionTableStatus(StatusConstant.LABEL_CNT_DST_RUNNING, StatusConstant.IDX_DST_READY, 333L, 1,
                "123", 12L, "20160808");
    }

    @Test
    @Transactional
    @Rollback(true)
    public void testDeleteExceptionTableStatus() {
        dao.deleteExceptionTableStatus("23", "20160808", 1);
    }

    @Test
    @Transactional
    @Rollback(true)
    public void testDeleteStatusFromLableStauts() {
        List<String> tableIdList = new ArrayList<String>();
        tableIdList.add("123");
        tableIdList.add("123");
        tableIdList.add("3221");
        tableIdList.add("135");
        dao.deleteStatusFromLableStauts("20160808", tableIdList);
    }

    @Test
    @Transactional
    @Rollback(false)
    public void testQueryUnsucccessfulTargetList() {
        dao.queryUnsucccessfulTargetList("20160808");
    }

    @Test
    @Transactional
    @Rollback(true)
    public void testDeleteTargetStatus() {
        dao.deleteTargetStatus("20160808", "ABC");
    }

    @Test
    @Transactional
    @Rollback(true)
    public void testDeleteLabelStatusByErrTableList() {
        List<String> tableIdList = new ArrayList<String>();
        tableIdList.add("123");
        tableIdList.add("123");
        tableIdList.add("3221");
        tableIdList.add("135");
        dao.deleteLabelStatusByErrTableList("20160808", tableIdList);
    }

    @Test
    @Transactional
    @Rollback(true)
    public void testUpdateLabelStatusByErrTableList() {
        List<String> tableIdList = new ArrayList<String>();
        tableIdList.add("123");
        dao.updateLabelStatusByErrTableList("20160808", tableIdList);
    }

    @Test
    @Transactional
    @Rollback(true)
    public void testQueryTargetStatusInfo() {
        dao.queryTargetStatusInfo("20160808");
    }

    @Test
    public void testUpdateLabelState() {
        dao.updateLabelState(StatusConstant.LABEL_CNT_DST_RUNNING, StatusConstant.LABEL_CNT_DST_RUNNING, "TEST", "123",
                "20160808", 123L, 111111L);
    }

    @Test
    @Transactional
    @Rollback(true)
	public void testDelLabelStatusByLabelList() {
		List<String> tableIdList = new ArrayList<String>();
		tableIdList.add("123");
		tableIdList.add("3221");
		tableIdList.add("135");
		dao.delLabelStatusByLabelList("20160808", tableIdList);
	}

}
