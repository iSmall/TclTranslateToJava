/* Copyright(c) 2017 AsiaInfo Technologies (China), Inc.
 * All Rights Reserved.
 * FileName: CommonService.java
 * Author:   <a href="mailto:xiongjie3@asiainfo.com">xiongjie3</a>
 * Date:     2017年2月28日 下午4:14:05
 * Description: DDL级操作
 */
package com.ai.tag.service;

import com.ai.tag.common.TagException;

/**
 * DDL操作<br>
 * 查询表，索引等
 *
 * @author xiongjie3
 */
public interface CommonService {

    /**
     * 
     * 功能描述: 查询表是否存在<br>
     *
     * @param tableName 表名称:形如schema.tableName,可以不带schema
     * @return
     * @throws TagException
     */
    public boolean isTableExists(String tableName) throws TagException;

    /**
     * 
     * 功能描述: 查询索引是否存在<br>
     *
     * @param indexName 索引名称
     * @return
     * @throws TagException
     */
    public boolean isIndexExists(String indexName) throws TagException;

    /**
     * 
     * 功能描述:删除表 <br>
     *
     * @param tableName 表名
     * @throws TagException
     */
    public void dropTable(String tableName) throws TagException;

    /**
     * 
     * 功能描述: 根据表结构描述创建表,如果要被创建的表存在，则先删除，再创建
     * </pre>
     * 如果不需要指定indexTabs，则给indexTabs赋0；如果不需要指定partitionKey</br>
     * 则给partitionKey赋0如果不需要指定distKey，则给dist_key赋0</br>
     * flag = 1:则当目标表存在时，删除它，然后重新建立</br>
     * flag = 0:则当目标表存在时，不删除，直接返回</br>
     *
     *
     *
     * @param tableName
     * @param fields
     * @param tableSpace
     * @param indexTabs
     * @param partitionKey
     * @param distKey
     * @param flag
     * @throws TagException
     */
    public void createTempTable(String tableName, String fields, String tableSpace, String indexTabs,
            String partitionKey, String distKey, int flag) throws TagException;

    /**
     * 
     * 功能描述: 清空数据<br>
     * 〈功能详细描述〉
     *
     * @param tableName
     * @throws TagException
     */
    public void truncateTable(String tableName) throws TagException;

    
    /**
     * 
     * 功能描述:创建表 <br>
     *
     * @param tableName
     * @param fields
     * @param tableSpace
     * @param tabsIndex
     * @param partitionKey
     * @param distribute
     * @throws TagException
     */
    public void createTable(String tableName, String fields, String tableSpace, String tabsIndex, String partitionKey,
            String distribute) throws TagException;
}
