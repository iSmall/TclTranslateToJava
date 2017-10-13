package com.ai.tag.service.impl;

import com.ai.tag.common.Job;
import com.ai.tag.common.StatusConstant;
import com.ai.tag.common.StringConstant;
import com.ai.tag.common.TagException;
import com.ai.tag.dao.IRestDealStateDao;
import com.ai.tag.utils.TagStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * 
 * 功能说明:用于在标签状态重置、指标状态重置、目标表状态重置,对应coc_tools_rerun_update_state.tcl<br> 
 * 数据流向: 通过标签配置表和标签与纵表列对应关系表配置表，根据数据源状态表更新纵表标签状态</br>
 * 标签配置表：</br>
 *  CI_APPROVE_STATUS</br>
 *  CI_LABEL_EXT_INFO</br>
 *  CI_LABEL_INFO</br>
 *  CI_MDA_SYS_TABLE</br>
 *  CI_MDA_SYS_TABLE_COLUMN</br>
 * 签与纵表列对应关系表：</br>
 *  CI_LABEL_VERTICAL_COLUMN_REL</br>
 * 数据源状态表：</br>
 *  DIM_COC_DATA_SOURCE_STATUS</br>
 *
 * @author chensf
 */
@Service("resetDealStateBiz")
public class ResetDealStateBizImpl extends BaseTaskExecution {

	private static final Logger TAGLOGGER = LoggerFactory.getLogger(ResetDealStateBizImpl.class);

	@Autowired
	protected IRestDealStateDao restDealStateDao;

	@Override
	public boolean executeTask(Job job) throws TagException {

		long start = System.currentTimeMillis();
		this.getInputParams(job);
		this.init(this.opTime, this.tbCycle);

		TAGLOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4：重置目标表状态表中异常状态>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		TAGLOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4.1：查询目标表状态表中非成功状态>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		List<Map<String, Object>> unsuccessfulStatusList = this.restDealStateDao.queryUnsuccessfulStatus(this.dataStatusDate);
		TAGLOGGER.info(">>>>>>>>>目标表状态表中非成功状态共:{}条", unsuccessfulStatusList.size());
		List<String> delDstWhere = new ArrayList<String>();
		List<String> delTableIdList = new ArrayList<String>();

		TAGLOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4.2：遍历目标表状态表中非成功状态记录>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		TAGLOGGER.info("删除状态表");
		this.processUnsuccessfulStatusList(unsuccessfulStatusList, delDstWhere, delTableIdList);

		TAGLOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4.4 : 删除异常的表状态>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		if (delDstWhere != null) {
			// 已经在4.2里删除了
			// this.restDealStateDao.deleteExceptionTableStatus(delDstWhere);
		}

		TAGLOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 5 : 重置指标状态>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		if (delTableIdList != null && delTableIdList.size() > 0) {
		    TAGLOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 5.1 : 从指标状态表中删除在已删除状态的指标宽表中的指标状态>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
			this.restDealStateDao.deleteStatusFromLableStauts(this.dataStatusDate, delTableIdList);
		}

		TAGLOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 5.2 : 查询状态不是成功的指标列表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		List<Map<String, Object>> unsuccessfulTargetList = this.restDealStateDao
				.queryUnsucccessfulTargetList(this.dataStatusDate);
		TAGLOGGER.info(">>>>>>>>>查询状态不是成功的指标列表共:{}条", unsuccessfulTargetList.size());
		this.processUnsuccessfulTargetList(unsuccessfulTargetList);

		TAGLOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 6 : 重置标签状态>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		TAGLOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 6.1 : 更新标签统计表对应的标签状态>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		if (delTableIdList != null && delTableIdList.size() > 0) {
			// 5
			this.restDealStateDao.updateLabelStatusByErrTableList(this.dataStatusDate, delTableIdList);
			// 4,
			this.restDealStateDao.deleteLabelStatusByErrTableList(this.dataStatusDate, delTableIdList);
		}
		TAGLOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 6.2 : 查询标签状态信息>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		List<String> delLabelIdList = new ArrayList<String>();
		List<Map<String, Object>> targetStatusInfoList = this.restDealStateDao.queryTargetStatusInfo(this.dataStatusDate);
		TAGLOGGER.info(">>>>>>>>>查询标签状态信息共:{}条", targetStatusInfoList.size());
		this.processTargetStatusInfoList(targetStatusInfoList, delLabelIdList);

		TAGLOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 6.3 : 删除异常标签状态>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		if (delLabelIdList != null && delLabelIdList.size() > 0) {
			this.restDealStateDao.delLabelStatusByLabelList(this.dataStatusDate, delLabelIdList);
		}
		TAGLOGGER.info("程序完成 !! ======>coc_tools_rerun_update_state 程序耗时{}分钟",
                (System.currentTimeMillis() - start) / 1000 / 60);
		return true;
	}

	/**
	 * 
	 * @param unsuccessfulStatusList
	 * @param delDstWhere
	 * @param delTableIdList
	 */
	private void processUnsuccessfulStatusList(List<Map<String, Object>> unsuccessfulStatusList,
			List<String> delDstWhere, List<String> delTableIdList) {
		for (Map<String, Object> unsuccessfulStatus : unsuccessfulStatusList) {
			// {LABEL_DST_TABLE_SCHEMA=-9, PID=36045524, TABLE_TYPE=3,
			// TABLE_ID=T_004, STATUS_ID=11,
			// IDX_DST_TABLE_NAME=DW_COC_INDEX_004_}
			String tableId = String.valueOf(unsuccessfulStatus.get("TABLE_ID")) ;
			int stausId = Integer.valueOf(String.valueOf(unsuccessfulStatus.get("STATUS_ID"))) ;
			String idxDstTabName = String.valueOf(unsuccessfulStatus.get("IDX_DST_TABLE_NAME")) ;
			String labDstTabName = String.valueOf(unsuccessfulStatus.get("LABEL_DST_TABLE_NAME")) ;
			String labDstTabSchema = String.valueOf(unsuccessfulStatus.get("LABEL_DST_TABLE_SCHEMA")) ;
			int tableType = Integer.valueOf(String.valueOf(unsuccessfulStatus.get("TABLE_TYPE"))) ;

			boolean needDelStatusFlag = false;

			if (labDstTabSchema == null || "".equals(labDstTabSchema) || "-9".equals(labDstTabSchema)) {
				labDstTabSchema = "sccoc";
			}

			String dstTableName = null;
			Integer dstTableSecStatus = null;
			String dropTmpTable = null;
			TAGLOGGER.info("判断");
			if (tableType == StatusConstant.INDEX_TABLE) {
				dstTableName = "sccoc." + idxDstTabName + this.opTime;
				dstTableSecStatus = StatusConstant.IDX_DST_SEC;
				dropTmpTable = "sccoc.T3_" + idxDstTabName + this.opTime;
			} else if (tableType == StatusConstant.LABEL_TABLE) {
				dstTableName = labDstTabSchema +"."+ labDstTabName + this.opTime;
				dstTableSecStatus = StatusConstant.LABEL_DST_SEC;
				dropTmpTable = "sccoc.T3_" + labDstTabName + this.opTime;
			} else if (tableType == StatusConstant.LABEL_CNT_TABLE) {
				dstTableName = labDstTabSchema +"."+ labDstTabName + "CNT_" + this.opTime;
				dstTableSecStatus = StatusConstant.LABEL_CNT_DST_SEC;
			} else if (tableType == StatusConstant.LABEL_STAT_TABLE) {
				needDelStatusFlag = true;
			} else {
				TAGLOGGER.error(">>>>>>不可识别的表类型,请检查DIM_TARGET_TABLE_STATUS ,表类型为:{}", tableType);
				continue;
			}
			TAGLOGGER.info("dstTableName" + ":" + dstTableName);
			if(this.baseDao.doesTableExist(TagStringUtils.getScheamWithTabName(dstTableName),
					TagStringUtils.getTabNameWithSchema(dstTableName))){
				needDelStatusFlag = this.baseDao.isEmptyTable(dstTableName);
			}else{
				needDelStatusFlag = true;
			}

			// 表存在并且有数据，更新目标表状态为成功
			TAGLOGGER.info("needDelStatusFlag" + needDelStatusFlag);
			if (!needDelStatusFlag) {
				// int newStatus, int oldStatus, int tableType, int tableId
			    TAGLOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4.3 : 更新异常的表状态>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
				this.restDealStateDao.updateExceptionTableStatus(dstTableSecStatus, stausId, this.batchNo, tableType,
						tableId, this.pid, this.dataStatusDate);
			} else {
				// 表不存在，删除目标表状态
				// String str = "\n or (TABLE_ID='%s' and DATA_DATE='%s' and
				// TABLE_TYPE=%s) ";
				// delDstWhere.add(String.format(str, tableId, this.opTime,
				// tableType));
				this.restDealStateDao.deleteExceptionTableStatus(tableId, this.dataStatusDate, tableType);
				delTableIdList.add(tableId);
			}
			// # 删除异常临时表
			if (dropTmpTable != null && 
					this.baseDao.doesTableExist(TagStringUtils.getScheamWithTabName(dropTmpTable),
					TagStringUtils.getTabNameWithSchema(dropTmpTable))) {
				this.baseDao.truncateTable(dropTmpTable);
				this.baseDao.dropTable(dropTmpTable);
			}
		}
	}

	/**
	 * 
	 * @param unsuccessfulTargetList
	 */
	private void processUnsuccessfulTargetList(List<Map<String, Object>> unsuccessfulTargetList) {

		for (Map<String, Object> unsuccessfulTarget : unsuccessfulTargetList) {
			// INDEX_CODE,DATA_STATUS,DATA_BATCH,value(PID,-9)
		    //modified by xiongjie 20170312 start
			String indexCode = String.valueOf(unsuccessfulTarget.get("INDEX_CODE")) ;
			int pid = Integer.valueOf(String.valueOf(unsuccessfulTarget.get("PID"))) ;
			//modified by xiongjie 20170312 end

			if (-9 == pid) {
				continue;
			}
			// 删除指标状态
			this.restDealStateDao.deleteTargetStatus(this.dataStatusDate, indexCode);
		}
	}

	/**
	 * 
	 * @param targetStatusInfoList
	 * @param delLabelIdList
	 */
	private void processTargetStatusInfoList(List<Map<String, Object>> targetStatusInfoList,
			List<String> delLabelIdList) {
		for (Map<String, Object> targetStatusInfo : targetStatusInfoList) {
			// P1.LABEL_ID ,P1.DATA_STATUS ,P1.DATA_BATCH ,value(P1.PID,'-9')
			// ,P1.EXCEPTION_DESC ,P2.LABEL_TYPE_ID,P3.IS_STAT_USER_NUM " +
			String labelId = String.valueOf(targetStatusInfo.get("LABEL_ID")) ;
			int dataStatus = Integer.valueOf(String.valueOf(targetStatusInfo.get("DATA_STATUS"))) ;
			int pid = Integer.valueOf(String.valueOf(targetStatusInfo.get("PID"))) ;
			String exceptionDesc = String.valueOf(targetStatusInfo.get("EXCEPTION_DESC")) ;
			int labelTypeId = Integer.valueOf(String.valueOf(targetStatusInfo.get("LABEL_TYPE_ID"))) ;
			int isStatUserNum = Integer.valueOf(String.valueOf(targetStatusInfo.get("IS_STAT_USER_NUM"))) ;

			String labelMsg = String.format("LABEL_ID=%s,PID=%s,DATA_STATUS=%s,原错误原因:%s", labelId, pid, dataStatus,
					exceptionDesc);
			if (-9 == pid) {
				TAGLOGGER.info("标签状态表中没有记录PID，无法处理该状态...{} 74450", labelMsg);
				continue;
			}
			String desc = String.format("重置标签状态，原状态 %s , 新状态LABEL_RULE_SEC，上次失败原因：%s;", dataStatus, exceptionDesc);

			if (dataStatus == StatusConstant.LABEL_RULE_FAIL) {
				TAGLOGGER.info("失败标签，删除...{} 74520", labelMsg);
				delLabelIdList.add(labelId);
			} else if (dataStatus == StatusConstant.LABEL_RULE_RUNNING) {
				TAGLOGGER.info("异常退出标签，删除...{}  74530", labelMsg);
				delLabelIdList.add(labelId);
			} else if (dataStatus == StatusConstant.LABEL_CNT_RUNNING || dataStatus == StatusConstant.LABEL_CNT_FAIL) {
				TAGLOGGER.info("标签统计异常退出标签，回退状态为规则生成成功...{}  74540", labelMsg);
				this.restDealStateDao.updateLabelState(StatusConstant.LABEL_RULE_SEC, dataStatus, desc, labelId,
						this.dataStatusDate, this.batchNo, this.pid);
			} else if (dataStatus == StatusConstant.LABEL_CUBE_RUNNING
					|| dataStatus == StatusConstant.LABEL_CUBE_FAIL) {
				TAGLOGGER.info("标标签汇总统计异常状态，回退异常标签状态...{}    74545", labelMsg);

				if (StringConstant.LABEL_TYPE_ONE_DIM == labelTypeId) {
					this.restDealStateDao.updateLabelState(StatusConstant.LABEL_CNT_SEC, dataStatus, desc, labelId,
							this.dataStatusDate, this.batchNo, this.pid);
				} else if (StringConstant.LABEL_TYPE_ATTR == labelTypeId) {
					if (isStatUserNum == 0) {
						this.restDealStateDao.updateLabelState(StatusConstant.LABEL_RULE_SEC, dataStatus, desc, labelId,
								this.dataStatusDate, this.batchNo, this.pid);
					} else if (isStatUserNum == 1) {
						delLabelIdList.add(labelId);
					}
				}
			} else if (dataStatus == StatusConstant.VERT_LABEL_RUNNING) {
				TAGLOGGER.info("更新纵表标签时异常退出标签，删除...{}  74550", labelMsg);
				delLabelIdList.add(labelId);
			} else if (dataStatus == StatusConstant.VERT_LABEL_FAIL) {
				TAGLOGGER.info("更新纵表标签时报错标签，删除...{}  74560", labelMsg);
				delLabelIdList.add(labelId);
			} else if (dataStatus == StatusConstant.LABEL_WAIT_RERUN) {
				TAGLOGGER.info("删除重跑标签时异常退出标签，删除...{}  74570", labelMsg);
				delLabelIdList.add(labelId);
			} else {
				TAGLOGGER.error("未被识别的标签状态，请修改代码增加判断处理...");
			}
		}
	}
}
