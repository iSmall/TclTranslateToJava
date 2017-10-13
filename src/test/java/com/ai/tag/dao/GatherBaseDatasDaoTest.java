/* Copyright(c) 2017 AsiaInfo Technologies (China), Inc.
 * All Rights Reserved.
 * FileName: GatherBaseDatasDaoTest.java
 * Author:   <a href="mailto:xiongjie3@asiainfo.com">xiongjie3</a>
 * Date:     2017年3月1日 下午5:17:13
 * Description: 指标层 基础数据汇总 dao 测试案例      
 */
package com.ai.tag.dao;

import static org.junit.Assert.assertTrue;

import com.ai.tag.BaseTagBizTest;
import com.ai.tag.common.StringConstant;
import com.ai.tag.common.TagConstant;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Resource;

import org.junit.Test;
import org.springframework.test.annotation.Rollback;
import org.springframework.transaction.annotation.Transactional;

/**
 * 指标层 基础数据汇总 dao 测试案例<br> 
 *
 * @author xiongjie3
 */

public class GatherBaseDatasDaoTest extends BaseTagBizTest{
    
    @Resource
    private IDwIndexDao dwIndexDao;
    
//    @Test
//    @Transactional
//    @Rollback(false)
//    public void testCheckReRunDataSource(){
//       assertTrue(dwIndexDao.getReRunDataSource("S_001','S_101','S_102").size()>0);
//    }
//    
//    @Test
//    @Transactional
//    @Rollback(true)
//    public void deleteReRunTagStatus(){
//        dwIndexDao.deleteReRunTagStatus("'B00005','B00008'", "20160224");
//    }
//    
//    
//    @Test
//    @Transactional
//    @Rollback(false)
//    public void queryReRunTagStatus(){
//        assertTrue(dwIndexDao.queryReRunTagStatus("B00190','B00129", "20160224")>0);
//    }
//    
//    @Test
//    @Transactional
//    @Rollback(true)
//    public void updateReRunTagStatusTest(){
//       assertTrue( dwIndexDao.updateReRunTagStatus(" 1=0 ", 1234, "20160601")>=0);
//    }
//    
//    
//    @Test
//    @Transactional
//    @Rollback(false)
//    public void selectLabeStatusTest(){
//        assertTrue(dwIndexDao.selectLabeStatus(" 1=0 ", "20160601")>=0);
//    }
//    
//    
//    @Test
//    @Transactional
//    @Rollback(true)
//    public void insertReRunLabelTableTest(){
//        assertTrue(dwIndexDao.insertReRunLabelTable("20160601",1234)>=0);
//    }
    
//    @Test
//    @Transactional
//    @Rollback(false)
//    public void queryMainTableDatasTest(){
//        assertTrue(dwIndexDao.queryMainTableDatas(TagConstant.DW_PRODUCT_DAY_TABLE_CODE, "20160601")>0);
//    }
    
//    @Test
//    @Transactional
//    @Rollback(true)
//    public void insertCurrentTagsTest(){
//        long pid = Thread.currentThread().getId();
//        Map<String, Object> params = new HashMap<String,Object>();
//        params.put("dataStatusDate", "20160601");
//        params.put("batchNo", 1234);
//        params.put("threadId", pid);
//        params.put("tbCycle", 1);
//        params.put("dateIso", "2016-06-01");
//        params.put("tabListStr",  "and T2.TARGET_TABLE_CODE in ('T_009') ");
//        assertTrue(dwIndexDao.insertCurrentTags(params)>0);
//    }
    
//    @Test
//    @Transactional
//    @Rollback(false)
//    public void queryCurrentBatchTagsTest(){
//        assertTrue(dwIndexDao.queryCurrentBatchTags(5457, "20160601").size()>0);
//    }
    
//    @Test
//    @Transactional
//    @Rollback(false)
//    public void queryTargetTableStatuTest(){
//        assertTrue(dwIndexDao.queryTargetTableStatu("T_001", "20160601")>0);
//    }
    
//    @Test
//    @Transactional
//    @Rollback(true)
//    public void updateTableIndexStatusFailTest(){
//        assertTrue(dwIndexDao.updateTableIndexStatusFail("updateTableIndexStatusFailTest", "T_002", "20160407", 4735)>0);
//    }
    
    @Test
    @Transactional
    @Rollback(true)
    public void updateDstStatus(){
        assertTrue(dwIndexDao.updateDstStatus(null, 0, 0, 0, null, 0, null, 0, null)>0);
    }
    
    
    
    
    
    

}
