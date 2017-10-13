/* Copyright(c) 2017 AsiaInfo Technologies (China), Inc.
 * All Rights Reserved.
 * FileName: GatherBaseDatasDaoImpl.java
 * Author:   <a href="mailto:xiongjie3@asiainfo.com">xiongjie3</a>
 * Date:     2017年2月22日 下午11:05:39
 * Description: 基础数据汇总
 */
package com.ai.tag.dao;

import com.ai.tag.common.TagException;

import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;

/**
 * 基础数据汇总<br>
 *
 * @author
 */
@Repository("iBaseDao")
public interface IBaseDao {

    /**
     * 
     * 功能描述: 获取下一个批次号<br>
     *
     * @return long sequence
     * @throws TagException
     */
    public long getNextSequence() throws TagException;

    /**
     * 功能描述: <br>
     * 〈功能详细描述〉
     *
     * @param dstTableName
     * @return
     * @see [相关类/方法](可选)
     * @since [产品/模块版本](可选)
     */
    public boolean isEmptyTable(@Param("tableName") String tableName);

    /**
     * 
     * @param tableName
     * @return
     */
    public boolean doesTableExist(@Param("schema") String schema, @Param("tableName") String tableName);

    /**
     * 
     * @param indexName
     * @return
     */
    public boolean doesIndexExist(@Param("schema") String schema, @Param("indexName") String indexName);

    /**
     * 
     * @param tableName
     */
    public void truncateTable(@Param("tableName") String tableName);

    /**
     * 
     * @param tableName
     */
    public void dropTable(@Param("tableName") String tableName);

    /**
     * 
     * @param tableName
     * @param fields
     * @param tableSpace
     * @param distKey
     * @param indx
     * @param partKey
     */
    public void createTable(@Param("tableName") String tableName, @Param("fields") String fields,
            @Param("tableSpace") String tableSpace, @Param("distKey") String distKey, @Param("indx") String indx,
            @Param("partKey") String partKey);

    /**
     * 
     * @param tableName
     * @param tempTable
     * @param tableSpace
     * @param distKey
     * @param indx
     * @param partKey
     */
    public void createTableFromTempTable(@Param("tableName") String tableName, @Param("tempTable") String tempTable,
            @Param("tableSpace") String tableSpace, @Param("distKey") String distKey, @Param("indx") String indx,
            @Param("partKey") String partKey);

    public void runstatsTable(@Param("tableName") String tableName);

    /**
     * 
     * 功能描述:关闭写日志 <br>
     *
     * @param tableName
     * @throws TagException
     */
    public void dbDisableLog(@Param("tableName") String tableName) throws TagException;

    /**
     * 
     * 功能描述:重命名 <br>
     *
     * @param schema
     * @param oldTable
     * @param newTable
     * @throws TagException
     */
    public void renameTable(@Param("schema") String schema, @Param("oldTable") String oldTable,
            @Param("newTable") String newTable) throws TagException;

    /**
     * 
     * 功能描述: 创建索引<br>
     *
     * @param indexName
     * @param dstTable
     * @param dstKey
     * @throws TagException
     */
    public void createIndex(@Param("indexName") String indexName, @Param("dstTable") String dstTable,
            @Param("distKey") String dstKey) throws TagException;

    /**
     * 删除索引
     * 
     * @param indexName
     */
    public void dropIndex(@Param("indexName") String indexName);

    /**
     * 
     * 功能描述: 执行reorg操作<br>
     *
     * @param schema
     * @param tableName
     * @throws TagException
     */
    public void reorgTable(@Param("schema") String schema, @Param("tableName") String tableName) throws TagException;

}
