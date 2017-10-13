package com.ai.tag.dao;

import java.util.List;
import java.util.Map;

import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface IGatherLabelUsersDao {

	/**
	 * 获取待处理数据
	 * 
	 * @param dataStatusDate
	 * @param batchNo
	 * @param effectDate
	 *            格式必须是yyyy-MM-dd
	 * @param tableCycle
	 * @return
	 */
	public List<Map<String, Object>> queryPendingDataList(@Param("date") String dataStatusDate,
			@Param("batchNo") long batchNo, @Param("effectDate") String effectDate,
			@Param("tableCycle") int tableCycle,@Param("tabListStr") String tabListStr);

	/**
	 * 
	 * @param dataStatusDate
	 * @param batchNo
	 * @param errMsg
	 * @param labelId
	 */
	public void updateCntLabelStatusFail(@Param("date") String dataStatusDate, @Param("batchNo") long batchNo,
			@Param("errMsg") String errMsg, @Param("labelId") int labelId);

	/**
	 * 
	 * @param newStatus
	 * @param batchNo
	 * @param desc
	 * @param oldStatus
	 * @param data
	 * @param tableType
	 * @param equalSql
	 * @param tableIdSql
	 */
	public void updateDstStatus(@Param("newStatus") int newStatus, @Param("newBatchNo") Long newBatchNo,
			@Param("pid") long pid, @Param("desc") String desc, @Param("oldStatus") int oldStatus,
			@Param("date") String dataStatusDate, @Param("tableType") int tableType,
			@Param("batchEqual") boolean batchEqual, @Param("batchNo") Long batchNo, @Param("tableId") Integer tableId);

	/**
	 * 
	 * @param data
	 */
	public void deleteTargetTableStatus(@Param("date") String dataStatusDate, @Param("tableId") int tableId);

	/**
	 * 
	 * @param tableId
	 * @param dataStatusDate
	 * @param tableType
	 * @param statusId
	 * @param batchNo
	 * @param pid
	 */
	public void insertDstStatus(@Param("tableId") int tableId, @Param("date") String dataStatusDate,
			@Param("tableType") int tableType, @Param("statusId") int statusId, @Param("batchNo") Long batchNo,
			@Param("pid") Long pid);

	/**
	 * 
	 * @param tableId
	 * @param statusId
	 * @param tableType
	 * @param batchNo
	 * @param batchEqual
	 * @return
	 */
	public long getDstCntByStatus(@Param("date") String dataStatusDate, @Param("tableId") int tableId,
			@Param("statusId") int statusId, @Param("tableType") int tableType, @Param("batchNo") Long batchNo,
			@Param("batchEqual") boolean batchEqual);

	/**
	 * 
	 * @param dataStatusDate
	 * @param tableId
	 * @param batchNo
	 * @param errMsg
	 */
	public void updateCntTableLabelStatusFail(@Param("date") String dataStatusDate, @Param("tableId") int tableId,
			@Param("batchNo") Long batchNo, @Param("errMsg") String errMsg);

	/**
	 * 
	 * @param dataStatusDate
	 * @param labelId
	 * @param batchNo
	 * @param pid
	 */
	public void updateTableLabelStatus(@Param("date") String dataStatusDate, @Param("labelId") String labelId,
			@Param("batchNo") Long batchNo, @Param("pid") Long pid);

	public void insertTmpTable(@Param("date") String dataStatusDate, @Param("tableName") String tableName, @Param("srcTableName") String srcTableName,
			@Param("fieldName") String fieldName, @Param("sumStr") String sumStr);

	/**
	 * 
	 * @param dataStatusDate
	 * @param labelId
	 * @param batchNo
	 */
	public void updateTableLabelStatusCnSec(@Param("date") String dataStatusDate, @Param("labelId") String labelId,
			@Param("batchNo") Long batchNo);
	
	
	public void updateTableLabelStatusDstSec(@Param("date") String dataStatusDate, @Param("tableId") int tableId,
			@Param("batchNo") Long batchNo);
}
