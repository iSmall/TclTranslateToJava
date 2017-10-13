package com.ai.tag.dao;

import java.util.List;
import java.util.Map;

import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface ILabelDataGenerationDao {

    /**
     * 更新已在标签状态表中标签状态为RUNNING
     * 
     * @param batchNo
     * @param pid
     * @param dateCycle
     * @param opTimeISO
     * @param tableCycle
     */
    public void updateLabelAsRunning(@Param("batchNo") Long batchNo, @Param("pid") long pid,
            @Param("dateCycle") String dateCycle, @Param("opTimeISO") String opTimeISO,
            @Param("tableCycle") int tableCycle);

    /**
     * 插入本批次可跑属性签子签
     * 
     * @param dateCycle
     * @param batchNo
     * @param pid
     * @param opTimeISO
     * @param tableCycle
     */
    public void insertSubLabels(@Param("dateCycle") String dateCycle, @Param("batchNo") long batchNo,
            @Param("pid") long pid, @Param("opTimeISO") String opTimeISO, @Param("tableCycle") int tableCycle);

    /**
     * 获取本批次状态为RUNNING的标签列表
     * 
     * @param dateCycle
     * @param batchNo
     * @return
     */
    public List<Map<String, Object>> getRunningLabelList(@Param("dateCycle") String dateCycle,
            @Param("batchNo") long batchNo);

    /**
     * 插入状态到目标表状态表
     * 
     * @param tableId
     * @param dataStatusDate
     * @param tableType
     * @param statusId
     * @param batchNo
     * @param pid
     */
    public void insertDstStatus(@Param("tableId") String tableId, @Param("date") String dataStatusDate,
            @Param("tableType") int tableType, @Param("statusId") int statusId, @Param("batchNo") Long batchNo,
            @Param("pid") Long pid);

    /**
     * 插入历史数据
     * 
     * @param dstTableName
     * @param srcTableName
     * @param date
     */
    public void insetDataIntoNewTempTable(@Param("dstTableName") String dstTableName,
            @Param("srcTableName") String srcTableName, @Param("date") String date);

    /**
     * 统计本统计周期总用户数
     * 
     * @param tableName
     */
    public long getUserCount(@Param("tableName") String tableName);

    /**
     * 插入目标表
     * 
     * @param dateCycle
     * @param lableId
     * @param sumStr
     * @param fromStr
     */
    public void insertTargetTable(@Param("tableName") String tableName, @Param("dateCycle") String dateCycle,
            @Param("lableId") int lableId, @Param("sumStr") String sumStr, @Param("fromStr") String fromStr);

    /**
     * 删除对应历史数据
     * 
     * @param tableName
     * @param labelIdList
     * @param dateCycle modified by xiongjie 20170312
     */
    public void deleteHistoryData(@Param("tableName") String tableName, @Param("labelIdList") String labelIdList,
            @Param("dateCycle") String dateCycle);

    /**
     * 插入目标表
     * 
     * @param destTableName
     * @param srcTableName
     * @param joinTableName
     * @param lastCycle
     * @param totalUserCount
     */
    public void insertTargetTable2(@Param("destTableName") String destTableName,
            @Param("srcTableName") String srcTableName, @Param("joinTableName") String joinTableName,
            @Param("lastCycle") String lastCycle, @Param("totalUserCount") long totalUserCount);

    /**
     * 更新最新数据周期
     * 
     * @param sql
     */
    public void updateDataCycle(@Param("sql") String sql);

    /**
     * 更新标签数据状态
     * 
     * @param labelIdList
     */
    public void updateLabelDataStatus(@Param("labelIdList") List<Integer> labelIdList);

    /**
     * 更新标签状态
     * 
     * @param dateCycle
     * @param batchNo
     * @param labelIdList
     */
    public void updateLabelStatus(@Param("dateCycle") String dateCycle, @Param("batchNo") long batchNo,
            @Param("labelIdList") List<Integer> labelIdList);

    /**
     * 删除目标表状态数据
     * 
     * @param batchNo
     */
    public void deleteDstTableStatus(@Param("batchNo") long batchNo);
}
