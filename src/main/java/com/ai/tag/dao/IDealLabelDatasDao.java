/* Copyright(c) 2017 AsiaInfo Technologies (China), Inc.
 * All Rights Reserved.
 * FileName: IDealLabelDatasDao.java
 * Author:   <a href="mailto:xiongjie3@asiainfo.com">xiongjie3</a>
 * Date:     2017年3月2日 下午2:13:47
 * Description: 标签层 数据处理 dao     
 */
package com.ai.tag.dao;

import com.ai.tag.common.TagException;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

/**
 * 标签层 数据处理 dao<br>
 *
 * @author xiongjie3
 */
@Repository
public interface IDealLabelDatasDao {

    /**
     * 程序预处理
     * @param sql
     * @throws TagException
     */
    public void updateDataCycle(@Param("sql") String sql) throws TagException;

    /**
     * 功能描述:获取标签列表 <br>
     *
     * @param dataStatusDate
     * @param dataDateIso
     * @param tabListStr
     * @param tableCycle
     * @return
     * @throws TagException
     */
    public List<Map<String, Object>> getTagsList(@Param("dataStatusDate") String dataStatusDate,
                                                 @Param("dataDateIso") String dataDateIso, @Param("tabListStr") String tabListStr,
                                                 @Param("tableCycle") int tableCycle) throws TagException;

    /**
     * 功能描述:获取所有子节点 <br>
     *
     * @param labelId
     * @param dataDateIso
     * @return
     * @throws TagException
     */
    public List<Map<String, Object>> getSonTagsRules(@Param("labelId") String labelId,
                                                     @Param("dataDateIso") String dataDateIso) throws TagException;

    /**
     * 功能描述: 更新标签状态<br>
     *
     * @param batchNo
     * @param threadId
     * @param labelId
     * @param dataStatusDate
     * @param dependIdxCnt
     * @param sql
     * @return
     * @throws TagException
     */
    public int updateTagStatusTable(@Param("batchNo") long batchNo, @Param("threadId") long threadId,
                                    @Param("labelId") long labelId, @Param("date") String dataStatusDate, @Param("cnt") int dependIdxCnt,
                                    @Param("sql") String sql) throws TagException;

    /**
     * 功能描述:插入标签状态表 <br>
     *
     * @param batchNo
     * @param threadId
     * @param labelId
     * @param dataStatusDate
     * @param dependIdxCnt
     * @param sql
     * @return
     * @throws TagException
     */
    public int insertTagStatusTable(@Param("batchNo") long batchNo, @Param("threadId") long threadId,
                                    @Param("labelId") long labelId, @Param("date") String dataStatusDate, @Param("cnt") int dependIdxCnt,
                                    @Param("sql") String sql) throws TagException;

    /**
     * 功能描述:获取所有的标签状态 <br>
     *
     * @param labelId
     * @param dataStatusDate
     * @param batchNo
     * @return
     * @throws TagException
     */
    public Integer getTotalTagsStatus(@Param("labelId") long labelId, @Param("date") String dataStatusDate,
                                      @Param("batchNo") long batchNo) throws TagException;

    /**
     * 功能描述:更改标签状态 <br>
     *
     * @param errorMsg
     * @param dataStatusDate
     * @param batchNo
     * @param sql
     * @return
     * @throws TagException
     */
    public int updateTagsStatusWithErrorMsg(@Param("errorMsg") String errorMsg, @Param("date") String dataStatusDate,
                                            @Param("batchNo") long batchNo, @Param("sql") String sql) throws TagException;

    /**
     * 功能描述:更新指标表状态，状态复原 <br>
     *
     * @param batchNo
     * @return
     * @throws TagException
     */
    public int recoverTagsStataus(@Param("batchNo") long batchNo) throws TagException;

    /**
     * 功能描述: 根据标签规则取出数据源表-指标宽表中，取出数据源表<br>
     *
     * @param condition
     * @param dataDateIso
     * @return
     * @throws TagException
     */
    public List<Map<String, Object>> getSrcTableByTagRules(@Param("sql") String condition,
                                                           @Param("date") String dataDateIso) throws TagException;

    /**
     * 功能描述: 更新指标表状态<br>
     *
     * @param batchNo
     * @param threadId
     * @param sql
     * @return
     * @throws TagException
     */
    public int updateTagsStatus(@Param("batchNo") long batchNo, @Param("threadId") long threadId,
                                @Param("sql") String sql) throws TagException;

    /**
     * 功能描述: 标签状态置为失败<br>
     *
     * @param errorMsg
     * @param date
     * @param batchNo
     * @param tableId
     * @return
     * @throws TagException
     */
    public int updateTagStatusFail(@Param("errorMsg") String errorMsg, @Param("date") String date,
                                   @Param("batchNo") long batchNo, @Param("tableId") String tableId) throws TagException;

    /**
     * 功能描述: 获取表状态<br>
     *
     * @param batchNo
     * @param sql
     * @return
     * @throws TagException
     */
    public Integer getTablesStatus(@Param("batchNo") long batchNo, @Param("sql") String sql) throws TagException;

    public Integer getTargetStatus(@Param("tableId") String tableId, @Param("date") String date) throws TagException;

    public Integer getCurrentWidthTable(@Param("tableId") String tableId, @Param("batchNo") long batchNo,
                                        @Param("date") String date) throws TagException;

    public int insertIntoTempTable(@Param("sql") String sql) throws TagException;

    public List<Map<String, Object>> getColsByDictTab(@Param("schema") String schema,
                                                      @Param("tableName") String tableName, @Param("sql") String sql) throws TagException;

    public int updateTagDataStatusOk(@Param("sql") String sql) throws TagException;

    public int editTagStatus(@Param("date") String date, @Param("batchNo") long batchNo, @Param("sql") String sql)
            throws TagException;

    public int recoverTagStatusTable(@Param("batchNo") long batchNo, @Param("sql") String sql,
                                     @Param("date") String date) throws TagException;

    public int recoverTagStatusTableOk(@Param("batchNo") long batchNo, @Param("date") String dataDateIso);

    public int updateDataCycleDate(@Param("date") String date) throws TagException;


    public int updateTagStatus(@Param("date") String date, @Param("batchNo") long batchNo, @Param("sql") String sql)
            throws TagException;

}
