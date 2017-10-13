/* Copyright(c) 2017 AsiaInfo Technologies (China), Inc.
 * All Rights Reserved.
 * FileName: DealLabelDalTest.java
 * Author:   <a href="mailto:xiongjie3@asiainfo.com">xiongjie3</a>
 * Date:     2017年3月3日 下午3:53:52
 * Description: //模块目的、功能描述      
 */
package com.ai.tag.dao;

import com.ai.tag.BaseTagBizTest;
import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.junit.Test;

/**
 *
 * @author xiongjie3
 */
public class DealLabelDalTest extends BaseTagBizTest{
    
    @Resource
    private IDealLabelDatasDao labelDatasDao;
    
    @Test
    public void testForeachTagList(){
        String tabListStr = " and CURR_APPROVE_STATUS_ID = '107' and DATA_STATUS_ID in (1,2)  ";
        List<Map<String, Object>> tags = labelDatasDao.getTagsList("20170304", "2017-03-04", tabListStr, 1);
        
        Map<String, List<String>> labelIdListStrArr = new HashMap<String, List<String>>();
        Map<String, String> tableNameArr = new HashMap<String, String>();
        
        for (Map<String, Object> eachRow : tags) {
            
            String tableId = String.valueOf(eachRow.get("TABLE_ID"));
            
            tableNameArr.put(tableId, String.valueOf(eachRow.get("TABLE_NAME")));
            
            if (labelIdListStrArr.containsKey(tableId)) {
                labelIdListStrArr.get(tableId).add("," + String.valueOf(eachRow.get("LABEL_ID")));
            } else {
                List<String> temp = new ArrayList<String>();
                temp.add("," + String.valueOf(eachRow.get("LABEL_ID")));
                labelIdListStrArr.put(tableId, temp);
            }
            
        }
        
        for (Map.Entry<String, String> map : tableNameArr.entrySet()) {
            String tableId = map.getKey();
            String where_label_list = Joiner.on("").join(labelIdListStrArr.get(tableId)).trim();
            if (where_label_list.startsWith(",")) {
                where_label_list = where_label_list.substring(1, where_label_list.length());
            } else if (where_label_list.endsWith(",")) {
                where_label_list = where_label_list.substring(0, where_label_list.lastIndexOf(","));
            }
            
            System.out.println(where_label_list);
            
        }
        
        
        
        
        
        
        
        
        
        
        
    }
    
    
    

}
