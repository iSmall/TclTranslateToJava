package com.ai.tag.dao;

import java.util.List;
import java.util.Map;

import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface IRestDealStateDao {

	/**
	 * 查询目标表状态表中非成功状态
	 * 
	 * @return
	 */
	public List<Map<String, Object>> queryUnsuccessfulStatus(@Param("date") String date);

	/**
	 * 更新异常的表状态
	 * 
	 * @param newStatus
	 * @param oldStatus
	 * @param batchNo
	 * @param tableType
	 * @param tableId
	 * @param pid
	 * @param dataStatusDate
	 */
	public void updateExceptionTableStatus(@Param("newStatus") int newStatus, @Param("oldStatus") int oldStatus,
			@Param("batchNo") long batchNo, @Param("tableType") int tableType, @Param("tableId") String tableId,
			@Param("pid") long pid, @Param("date") String dataStatusDate);

	/**
	 * 删除异常的表状态
	 * 
	 * @param condition
	 */
	public void deleteExceptionTableStatus(@Param("tableId") String tableId, @Param("date") String date,
			@Param("tableType") int tableType);

	/**
	 * 从指标状态表中删除在已删除状态的指标宽表中的指标状态
	 * 
	 * @param delTableIdList
	 */
	public void deleteStatusFromLableStauts(@Param("date") String dataStatusDate,
			@Param("tableIdList") List<String> delTableIdList);

	/**
	 * 查询状态不是成功的指标列表
	 * 
	 * @return
	 */
	public List<Map<String, Object>> queryUnsucccessfulTargetList(@Param("date")String date);

	/**
	 * 删除指标状态
	 * 
	 * @param indexCode
	 */
	public void deleteTargetStatus(@Param("date") String dataStatusDate, @Param("indexCode") String indexCode);

	/**
	 * 从标签状态表中删除在已删除状态的标签宽表中的标签状态
	 * 
	 * @param delTableIdList
	 */
	public void deleteLabelStatusByErrTableList(@Param("date") String dataStatusDate,
			@Param("tableIdList") List<String> delTableIdList);

	/**
	 * 更新标签统计表对应的标签状态
	 * 
	 * @param dataStatusDate
	 * @param delTableIdList
	 */
	public void updateLabelStatusByErrTableList(@Param("date") String dataStatusDate,
			@Param("tableIdList") List<String> delTableIdList);

	/**
	 * 查询标签状态信息
	 * 
	 * @return
	 */
	public List<Map<String, Object>> queryTargetStatusInfo(@Param("date")String date);

	/**
	 * 更新异常的标签状态为上一步成功状态
	 * 
	 * @param newStatus
	 * @param oldStatus
	 * @param desc
	 * @param labelID
	 */
	public void updateLabelState(@Param("newStatus") int newStatus, @Param("oldStatus") int oldStatus,
			@Param("desc") String desc, @Param("labelID") String labelID, @Param("date") String dataStatusDate,
			@Param("batchNo") long batchNo, @Param("pid") long pid);

	/**
	 * 删除异常标签状态
	 * 
	 * @param dataStatusDate
	 * @param delLabelIdList
	 */
	public void delLabelStatusByLabelList(@Param("date") String dataStatusDate,
			@Param("tableIdList") List<String> delLabelIdList);
}
