/* Copyright(c) 2017 AsiaInfo Technologies (China), Inc.
 * All Rights Reserved.
 * FileName: IDwIndexDao.java
 * Author:   <a href="mailto:xiongjie3@asiainfo.com">xiongjie3</a>
 * Date:     2017年2月26日 下午8:37:44
 * Description: 指标层 基础数据汇总
 */
package com.ai.tag.dao;

import com.ai.tag.common.TagException;

import java.util.List;
import java.util.Map;

import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;

/**
 * 指标层 基础数据汇总<br>
 * 指标层 基础数据汇总dao
 *
 * @author xiongjie3
 */
@Repository
public interface IDwIndexDao {

    /**
     * 
     * 功能描述:检查重跑接口数据源表 <br>
     *
     * @param srcTables 数据源表
     * @return
     * @throws TagException
     */
    public List<String> getReRunDataSource(@Param("srcTables") String srcTables) throws TagException;

    /**
     * 
     * 功能描述: 删除重跑指标状态<br>
     *
     * @param redoIndex
     * @param dataStatusDate
     * @throws TagException
     */
    public int deleteReRunTagStatus(@Param("indexCode") String redoIndex, @Param("date") String dataStatusDate)
            throws TagException;

    /**
     * 
     * 功能描述: 查询重跑指标状态<br>
     *
     * @param redoIndex
     * @param dataStatusDate
     * @return
     * @throws TagException
     */
    public int queryReRunTagStatus(@Param("indexCode") String redoIndex, @Param("date") String dataStatusDate)
            throws TagException;

    /**
     * 
     * 功能描述: 更新依赖重跑指标的标签状态为待重跑<br>
     *
     * @param whereDependIdxListStr
     * @param batchNo
     * @param date
     * @throws TagException
     */
    public int updateReRunTagStatus(@Param("idxStrs") String whereDependIdxListStr, @Param("batchNo") long batchNo,
            @Param("date") String date) throws TagException;

    /**
     * 
     * 功能描述: 查询标签状态<br>
     *
     * @param whereDependIdxListStr
     * @param date
     * @return
     * @throws TagException
     */
    public int selectLabeStatus(@Param("idxStrs") String whereDependIdxListStr, @Param("date") String date)
            throws TagException;

    /**
     * 
     * 功能描述: 标签插入标签重跑表<br>
     *
     * @param dataStatusDate
     * @param batchNo
     * @throws TagException
     */
    public int insertReRunLabelTable(@Param("date") String dataStatusDate, @Param("batchNo") long batchNo)
            throws TagException;

    /**
     * 
     * 功能描述: 检查主表是否准备好<br>
     *
     * @param dwProductCode
     * @param dataStatusDate
     * @return
     * @throws TagException
     */
    public int queryMainTableDatas(@Param("dwCode") String dwProductCode, @Param("date") String dataStatusDate)
            throws TagException;

    /**
     * 
     * 功能描述: 插入本次要跑的指标，状态为运行中<br>
     *
     * @param dataStatusDate
     * @param batchNo
     * @param threadId
     * @param effectDate
     * @param dealTabListStr
     * @throws TagException
     */
    public int insertCurrentTags(@Param("param") Map<String,Object> map) throws TagException;

    /**
     * 
     * 功能描述:获取本次要跑指标信息 <br>
     *
     * @param batchNo
     * @param dataStatusDate
     * @return
     * @throws TagException
     */
    public List<Map<String, Object>> queryCurrentBatchTags(@Param("batchNo") long batchNo,
            @Param("date") String dataStatusDate) throws TagException;

    /**
     * 
     * 功能描述: 查询目标表状态<br>
     *
     * @param tableCode
     * @param opTime
     * @return
     * @throws TagException
     */
    public Integer queryTargetTableStatu(@Param("tableCode") String tableCode, @Param("date") String opTime)
            throws TagException;

    /**
     * 
     * 功能描述: 指标状态置为失败<br>
     *
     * @param errorMsg
     * @param tableCode
     * @param dataStatusDate
     * @param currBatch
     * @throws TagException
     */
    public int updateTableIndexStatusFail(@Param("errorMsg") String errorMsg, @Param("tableCode") String tableCode,
            @Param("date") String dataStatusDate, @Param("currBatch") long currBatch) throws TagException;

    /**
     * 
     * 功能描述: 写入COC目标表状态表<br>
     *
     * @param tableCode
     * @param tableType
     * @param oldStatus
     * @param newStatus
     * @param dataDate
     * @param batch
     * @param batchEqual
     * @param thradId
     * @param desc
     * @throws TagException
     */
    public int updateDstStatus(@Param("tableCode") String tableCode, @Param("tableType") int tableType,
            @Param("oldStatus") int oldStatus, @Param("newStatus") int newStatus, @Param("date") String dataDate,
            @Param("batch") long batch, @Param("batchEqual") String batchEqual, @Param("threadId") long threadId,
            @Param("desc") String desc) throws TagException;

    /**
     * 
     * 功能描述:获取目标表状态 <br>
     *
     * @param tableCode
     * @param batch
     * @param dataStatusDate
     * @return
     * @throws TagException
     */
    public int getTargetTableStatus(@Param("tableCode") String tableCode, @Param("batch") long batch,
            @Param("date") String dataStatusDate) throws TagException;

    /**
     * 
     * 功能描述: 插入临时表<br>
     *
     * @param t1DstTable
     * @param insertSql
     * @throws TagException
     */
    public int insertTempTableData(@Param("t1DstTable") String t1DstTable, @Param("insertSql") String insertSql)
            throws TagException;

    /**
     * 
     * 功能描述:获取上一批次指标字段和类型 <br>
     *
     * @param cmdPrefix
     * @param schema
     * @param tableName
     * @param expColsSql
     * @return
     * @throws TagException
     */
    public List<Map<String, String>> getColsByDictTabSqlNamespace(@Param("schema") String schema,
            @Param("tableName") String tableName, @Param("expColsSql") String expColsSql) throws TagException;

    public int insertDstTableData(@Param("sql") String sql) throws TagException;

    /**
     * 
     * 功能描述: 写入COC目标表状态表<br>
     *
     * @param tableCode 目标表ID
     * @param dataDate 数据日期
     * @param tableType 表类型，分标签宽表、指标宽表、标签统计宽表
     * @param status 状态
     * @param batch 批次编码，通过序列获取，用于进程标识
     * @param threadId 写入状态的TCL的进程ID，用于在重跑前状态重置脚本中RUNNING进程判定
     * @throws TagException
     */
    public int insertDstStatus(@Param("tableCode") String tableCode, @Param("tableType") int tableType,
            @Param("status") int status, @Param("date") String dataDate, @Param("batch") long batch,
            @Param("threadId") long threadId) throws TagException;

    /**
     * 
     * 功能描述: 修改指标状态<br>
     *
     * @param date
     * @param batch
     * @param idxList
     * @throws TagException
     */
    public int updateTagsStatus(@Param("status4Set") int status4Set, @Param("statusCon") int statusCon,
            @Param("date") String date, @Param("batch") long batch, @Param("idxList") String idxList)
            throws TagException;

    public int recoverTagStatusOk(@Param("date") String date, @Param("batchNo") long batchNo,
            @Param("sql") String sql) throws TagException;

}
