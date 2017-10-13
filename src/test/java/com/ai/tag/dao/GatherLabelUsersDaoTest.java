package com.ai.tag.dao;

import com.ai.tag.BaseTagBizTest;

import javax.annotation.Resource;

import org.junit.Test;
import org.springframework.test.annotation.Rollback;
import org.springframework.transaction.annotation.Transactional;

public class GatherLabelUsersDaoTest extends BaseTagBizTest {

    @Resource
    private IGatherLabelUsersDao dao;

    @Test
    @Transactional
    @Rollback(false)
    public void testQueryPendingDataList() {
        dao.queryPendingDataList("20170101", 1L, "20170101", 1, "");
    }

    @Test
    @Transactional
    @Rollback(true)
    public void testUpdateCntLabelStatusFail() {
        dao.updateCntLabelStatusFail("20170101", 123L, "TEST", 3);
    }

    @Test
    @Transactional
    @Rollback(true)
    public void testUpdateDstStatus() {
        dao.updateDstStatus(1, 2L, 1L, "TEST", 1, "20170101", 2, true, 1L, 23444);
    }

    @Test
    @Transactional
    @Rollback(true)
    public void testDeleteTargetTableStatus() {
        dao.deleteTargetTableStatus("20170101", 123);
    }

    @Test
    @Transactional
    @Rollback(true)
    public void testInsertDstStatus() {
        // dao.insertDstStatus("TEST123", "20170101", 1, 1, 123L, 123L);
    }

    @Test
    @Transactional
    @Rollback(true)
    public void testGetDstCntByStatus() {
        dao.getDstCntByStatus("20170101", 123, 1, 1, 2L, true);
    }

    @Test
    @Transactional
    @Rollback(true)
    public void testUpdateCntTableLabelStatusFail() {
        dao.updateCntTableLabelStatusFail("20170101", 123, 1L, "获取表状态异常，跳过处理");
    }

    @Test
    @Transactional
    @Rollback(true)
    public void testUpdateTableLabelStatus() {
        dao.updateTableLabelStatus("20170101", "12345", 1L, 1L);
    }

    @Test
    @Transactional
    @Rollback(true)
    public void testUpdateTableLabelStatusCnSec() {
        dao.updateTableLabelStatusCnSec("20170101", "1", 1L);
    }

    @Test
    @Transactional
    @Rollback(true)
    public void testUpdateTableLabelStatusDstSec() {
        dao.updateTableLabelStatusDstSec("20170101", 12344, 1L);
    }

    @Test
    @Transactional
    @Rollback(true)
    public void testInsertTmpDate() {
        dao.insertTmpTable("2017-03-04 ","sccoc.DW_COC_LABEL_USER_005_CNT_20170304",
                "sccoc.DW_COC_LABEL_USER_005_20170304",
                ",L1_02_06_05,L10_01_01,L3_03_05_02,L3_03_05_05,L3_03_05_01,L3_03_06_03,L10_04_01,L1_02_06_13,L1_02_06_07,L3_03_06_05,L1_02_06_08,L1_02_06_11,L3_03_06_04,L1_02_06_12,L1_02_06_10,L3_03_04_03,L3_03_06_01,L3_04_01,L1_02_06_06,L1_02_06_14,L3_04_02,L1_02_06_01,L3_03_04_01,L3_03_05_04,L1_02_06_04,L1_02_06_09,L3_03_04_04,L3_03_04_02,L1_02_06_03,L3_03_06_02,L1_02_06_02,L3_03_05_03",
                ",SUM(L1_02_06_05) ,SUM(L10_01_01) ,SUM(L3_03_05_02) ,SUM(L3_03_05_05) ,SUM(L3_03_05_01) ,SUM(L3_03_06_03) ,SUM(L10_04_01) ,SUM(L1_02_06_13) ,SUM(L1_02_06_07) ,SUM(L3_03_06_05) ,SUM(L1_02_06_08) ,SUM(L1_02_06_11) ,SUM(L3_03_06_04) ,SUM(L1_02_06_12) ,SUM(L1_02_06_10) ,SUM(L3_03_04_03) ,SUM(L3_03_06_01) ,SUM(L3_04_01) ,SUM(L1_02_06_06) ,SUM(L1_02_06_14) ,SUM(L3_04_02) ,SUM(L1_02_06_01) ,SUM(L3_03_04_01) ,SUM(L3_03_05_04) ,SUM(L1_02_06_04) ,SUM(L1_02_06_09) ,SUM(L3_03_04_04) ,SUM(L3_03_04_02) ,SUM(L1_02_06_03) ,SUM(L3_03_06_02) ,SUM(L1_02_06_02) ,SUM(L3_03_05_03)");
    }
}
