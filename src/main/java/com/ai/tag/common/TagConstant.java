/* Copyright(c) 2017 AsiaInfo Technologies (China), Inc.
 * All Rights Reserved.
 * FileName: TagConstant.java
 * Author:   <a href="mailto:xiongjie3@asiainfo.com">xiongjie3</a>
 * Date:     2017年2月26日 下午4:29:15
 * Description: 常量类
 */
package com.ai.tag.common;

/**
 * 常量类<br> 
 *
 * @author xiongjie3
 */
public class TagConstant {
    /**
     * 日周期
     */
    public static final int DAY_FLAG = 1;
    
    
    /**
     * 月周期
     */
    public static final int MONTH_FLAG = 2;
    
    /**
     * 日用户表在数据源状态表的CODE码，用于分批次中读取主表状态
     */
    public static final String DW_PRODUCT_DAY_TABLE_CODE = "DS_005";
//    public static final String DW_PRODUCT_DAY_TABLE_CODE = "S_001";
    
    
    /**
     * 月用户表在数据源状态表的CODE码，用于分批次中读取主表状态，
     * 如果主表在指标源表维表用有配置，用维表的TARGET_TABLE_CODE字段值，
     * 否则可以自己编一个CODE不用插到维表中
     */
    public static final String DW_PRODUCT_MONTH_TABLE_CODE = "MS_005";
    
    /**
     * coc 指标层 基础数据汇总
     */
    public static final String TAG_BASE_DATA_GATHER = "dw_index";
    
    public static final String TAG_RESET_DEAL_STATE="reset_deal";

}
