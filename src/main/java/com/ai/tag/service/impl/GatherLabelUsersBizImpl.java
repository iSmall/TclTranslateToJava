package com.ai.tag.service.impl;

import com.ai.tag.common.Job;
import com.ai.tag.common.StatusConstant;
import com.ai.tag.common.StringConstant;
import com.ai.tag.common.TagException;
import com.ai.tag.dao.IGatherLabelUsersDao;
import com.ai.tag.utils.DateFormatUtils;
import com.ai.tag.utils.TagStringUtils;

import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

/**
 * 
 * 功能说明: 在维度上汇总0/1型标签的用户数,对应coc_d_label_user_count_yyyymmdd.tcl<br>
 *
 * @author chensf
 */
@Service("gatherLabelUsersBiz")
public class GatherLabelUsersBizImpl extends BaseTaskExecution {
    private static final Logger TAGLOGGER = LoggerFactory.getLogger(GatherLabelUsersBizImpl.class);

    @Autowired
    protected IGatherLabelUsersDao gatherLabelUsersDao;

    @Override
    public boolean executeTask(Job job) throws TagException {

        long start = System.currentTimeMillis();

        // 初始化
        this.getInputParams(job);
        this.init(this.opTime, this.tbCycle);

        TAGLOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 1.2 : 确定宽表范围>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        StringBuilder tabListStr = new StringBuilder("");

        if (!StringUtils.isEmpty(tbCode)) {
            StringBuilder tabList = new StringBuilder();
            String[] tbCodes = tbCode.split(",");
            for (String str : tbCodes) {
                tabList.append(" ").append(str).append(",");
            }

            tabListStr.append(" and T1.TABLE_ID in (").append(TagStringUtils.trimRightByChar(tabList.toString(), ","))
                    .append(" )");
        }

        TAGLOGGER.info(
                ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 2 : 从 DIM_COC_LABEL_TABLE 中取出符合条件的表名名称、周期等，放入数组中>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        TAGLOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 2.1 :获取需要处理的数据>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        List<Map<String, Object>> pendingDataList = this.gatherLabelUsersDao.queryPendingDataList(this.dataStatusDate,
                this.batchNo, this.dataDateIso, this.tbCycle, tabListStr.toString());
        TAGLOGGER.info(">>>>>>>>>数据共:{}条", pendingDataList.size());
        boolean needRun = this.validateNeedRunPendingData(pendingDataList);
        if (!needRun) {
            TAGLOGGER.info("本批次没有需要统计用户数的标签...直接退出   21045");
            return true;
        }

        // table信息
        Map<Integer, Integer> tableUpdateCycle = new HashMap<Integer, Integer>();
        Map<Integer, String> tableNameMap = new HashMap<Integer, String>();
        // schema名
        Map<Integer, String> tableSchemaMap = new HashMap<Integer, String>();
        // select信息
        Map<Integer, StringBuilder> tableDDLColMap = new HashMap<Integer, StringBuilder>();
        Map<Integer, StringBuilder> insertColMap = new HashMap<Integer, StringBuilder>();
        Map<Integer, StringBuilder> labelIdMap = new HashMap<Integer, StringBuilder>();
        // 标签类型拼语句
        Map<Integer, StringBuilder> sumColStrMap = new HashMap<Integer, StringBuilder>();
        Map<Integer, Boolean> tableMuiltFlagMap = new HashMap<Integer, Boolean>();

        TAGLOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>3、loop每个标签，进行处理>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        TAGLOGGER.info(
                ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 3.1 : 获取每个表的表信息,label信息,字段信息...>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        this.processPendingDataList(pendingDataList, tableUpdateCycle, tableNameMap, tableSchemaMap, tableDDLColMap,
                insertColMap, labelIdMap, tableMuiltFlagMap, sumColStrMap);

        TAGLOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 3.2 : 处理每个表..>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        this.processEachTables(tableUpdateCycle, tableNameMap, tableSchemaMap, tableDDLColMap, insertColMap, labelIdMap,
                tableMuiltFlagMap, sumColStrMap);

        TAGLOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4 : 状态表状态归位>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        TAGLOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4.1 : 源表状态归位>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        this.gatherLabelUsersDao.updateDstStatus(StatusConstant.LABEL_DST_SEC, this.batchNo, this.pid, "",
                StatusConstant.LABEL_DST_USING, dataStatusDate, StringConstant.TABLE_TYPE_LABEL_TABLE, true, batchNo,
                null);
        TAGLOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4.2 : 目标表状态归位>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        this.gatherLabelUsersDao.updateDstStatus(StatusConstant.LABEL_CNT_DST_FAIL, this.batchNo, this.pid, "",
                StatusConstant.LABEL_CNT_DST_RUNNING, dataStatusDate, StringConstant.TABLE_TYPE_LABEL_CNT_TABLE, true,
                batchNo, null);
        TAGLOGGER.info("程序完成 !! ======>coc_d_dw_label_table_rules_yyyymmdd 程序耗时{}分钟",
                (System.currentTimeMillis() - start) / 1000 / 60);
        return true;
    }

    /**
     * 判断是否有数据需要处理
     * 
     * @param pendingDataList
     * @return
     */
    private boolean validateNeedRunPendingData(List<Map<String, Object>> pendingDataList) {
        boolean needRunLabelFlag = false;
        for (Map<String, Object> map : pendingDataList) {
            int dataStatus = (int) map.get("DATA_STATUS");
            if (dataStatus == StatusConstant.LABEL_RULE_SEC || dataStatus == StatusConstant.LABEL_CNT_FAIL) {
                needRunLabelFlag = true;
            }
        }
        return needRunLabelFlag;
    }

    /**
     * 
     * @param pendingDataList
     * @param tableDDLColMap
     * @param insertColLis
     * @param labelIdMap
     * @param tableMuiltFlag
     * @param sumColStrMap
     */
    private void processPendingDataList(List<Map<String, Object>> pendingDataList,
            Map<Integer, Integer> tableUpdateCycle, Map<Integer, String> tableNameMap,
            Map<Integer, String> tableSchemaMap, Map<Integer, StringBuilder> tableDDLColMap,
            Map<Integer, StringBuilder> insertColMap, Map<Integer, StringBuilder> labelIdMap,
            Map<Integer, Boolean> tableMuiltFlag, Map<Integer, StringBuilder> sumColStrMap) {

        for (Map<String, Object> map : pendingDataList) {
            int tableId = (int) map.get("TABLE_ID");
            String tableSchema = (String) map.get("TABLE_SCHEMA");
            String tableName = (String) map.get("TABLE_NAME");
            map.get("COLUMN_ID");
            String columnName = (String) map.get("COLUMN_NAME");
            int labelTypeId = (int) map.get("LABEL_TYPE_ID");
            String dataType = (String) map.get("DATA_TYPE");
            int labelId = (int) map.get("LABEL_ID");
            int cycle = (int) map.get("UPDATE_CYCLE");

            // # 获取table信息
            tableUpdateCycle.put(tableId, cycle);
            tableNameMap.put(tableId, tableName);

            // # schema名
            if ("-9".equals(tableSchema)) {
                tableSchemaMap.put(tableId, "sccoc");
            } else {
                tableSchemaMap.put(tableId, tableSchema);
            }

            if ("-9".equals(dataType)) {
                String errorMsg = "字段类型配置异常,请排查异常数据后从标签状态表删除本批次状态重跑,请检查DIM_COLUMN_DATA_TYPE字段类型,label_id={},column_name={},LABEL_TYPE_ID={} 21060";
                TAGLOGGER.error(errorMsg, labelId, columnName);
                this.gatherLabelUsersDao.updateCntLabelStatusFail(this.dataStatusDate, this.batchNo, errorMsg, labelId);
                continue;
            }
            // #拼字段信息和select信息
            if (tableDDLColMap.containsKey(tableId)) {
                tableDDLColMap.get(tableId).append(","+columnName + " " + dataType);
            } else {
                StringBuilder sb = new StringBuilder(","+columnName + " " + dataType);
                tableDDLColMap.put(tableId, sb);
            }

            if (insertColMap.containsKey(tableId)) {
                insertColMap.get(tableId).append(",").append(columnName);
            } else {
                StringBuilder sb = new StringBuilder("," + columnName);
                insertColMap.put(tableId, sb);
            }
            
            if (labelIdMap.containsKey(tableId)) {
                labelIdMap.get(tableId).append(",").append(labelId);
            } else {
                StringBuilder sb = new StringBuilder("," + labelId);
                labelIdMap.put(tableId, sb);
            }

            // # 根据标签类型拼语句
            if (labelTypeId == StringConstant.LABEL_TYPE_ONE_DIM) {
                if (sumColStrMap.containsKey(tableId)) {
                    sumColStrMap.get(tableId).append(",SUM(" + columnName + ") \n");
                } else {
                    StringBuilder sb = new StringBuilder(",SUM(" + columnName + ") \n");
                    sumColStrMap.put(tableId, sb);
                }

                tableMuiltFlag.put(tableId, false);
            } else if (labelTypeId == StringConstant.LABEL_TYPE_ATTR) {

                if (sumColStrMap.containsKey(tableId)) {
                    sumColStrMap.get(tableId).append("," + columnName + " \n");
                } else {

                    StringBuilder sb = new StringBuilder("," + columnName + " \n");
                    sumColStrMap.put(tableId, sb);
                }
                tableMuiltFlag.put(tableId, true);
            }

        }
    }

    private void processEachTables(Map<Integer, Integer> tableUpdateCycle, Map<Integer, String> tableNameMap,
            Map<Integer, String> tableSchemaMap, Map<Integer, StringBuilder> tableDDLColMap,
            Map<Integer, StringBuilder> insertColMap, Map<Integer, StringBuilder> labelIdMap,
            Map<Integer, Boolean> tableMuiltFlagMap, Map<Integer, StringBuilder> sumColStrMap) {
        for (Integer currTableId : tableDDLColMap.keySet()) {
            TAGLOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 3.2.1 : 处理源/目标表名>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            String targetRunTime = null;
            // String dwProductTabName = null;
            String operationTime = null;
            try {
                if (this.tbCycle == StringConstant.CYCLE_DEF_FT_MONTHLY) {
                    targetRunTime = DateFormatUtils.dateToStr_YYYYMM(DateFormatUtils.strToDate_YYYYMM(this.opTime));
                    // dwProductTabName = StringConstant.DW_PRODUCT_MONTH_TABLE_NAME + targetRunTime;
                    operationTime = DateFormatUtils.dateToStr_YYYY_MM_DD(DateFormatUtils.strToDate_YYYYMMDD(this.opTime+"01"));
                } else if (this.tbCycle == StringConstant.CYCLE_DEF_FT_DAILY) {
                    targetRunTime = this.opTime;
                    // dwProductTabName = StringConstant.DW_PRODUCT_DAY_TABLE_NAME + targetRunTime;
                    operationTime = DateFormatUtils
                            .dateToStr_YYYY_MM_DD(DateFormatUtils.strToDate_YYYYMMDD(this.opTime));
                }
            } catch (ParseException e) {
                throw new TagException("时间转换异常："+e.getMessage());
            }
            // # 源/目标表名
            String srcTable = tableSchemaMap.get(currTableId) + "." + tableNameMap.get(currTableId) + "_"
                    + targetRunTime;
            String dstTable = tableSchemaMap.get(currTableId) + "." + tableNameMap.get(currTableId) + "_CNT_"
                    + targetRunTime;

            if (tableMuiltFlagMap.get(currTableId) != null && !tableMuiltFlagMap.get(currTableId)) {
                // # 源表状态还原
                this.gatherLabelUsersDao.updateDstStatus(StatusConstant.LABEL_DST_SEC, this.batchNo, this.pid, "",
                        StatusConstant.LABEL_DST_USING, this.dataStatusDate, StringConstant.TABLE_TYPE_LABEL_TABLE,
                        true, this.batchNo, null);
                this.gatherLabelUsersDao.deleteTargetTableStatus(this.dataStatusDate, currTableId);

                this.gatherLabelUsersDao.insertDstStatus(currTableId, this.dataStatusDate,
                        StringConstant.TABLE_TYPE_LABEL_CNT_TABLE, StatusConstant.LABEL_CNT_DST_RUNNING, batchNo, pid);

                TAGLOGGER.info(
                        ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4.2 : 获取本批次宽表状态>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                long dstTabCntRes = this.gatherLabelUsersDao.getDstCntByStatus(this.dataStatusDate, currTableId,
                        StatusConstant.LABEL_CNT_DST_RUNNING, StringConstant.TABLE_TYPE_LABEL_CNT_TABLE, batchNo, true);
                this.gatherLabelUsersDao.updateDstStatus(StatusConstant.LABEL_DST_USING, this.batchNo, this.pid, "",
                        StatusConstant.LABEL_DST_SEC, this.dataStatusDate, StringConstant.TABLE_TYPE_LABEL_TABLE, false,
                        null, currTableId);
                long usedTabCntRes = this.gatherLabelUsersDao.getDstCntByStatus(this.dataStatusDate, currTableId,
                        StatusConstant.LABEL_DST_USING, StringConstant.TABLE_TYPE_LABEL_TABLE, this.batchNo, true);

                if (dstTabCntRes < 0 || usedTabCntRes < 0) {
                    String msg = "获取表状态异常，跳过处理...target_table_code={},TABLE_TYPE={}$TABLE_TYPE_ARRAY(LABEL_TABLE),{}$TABLE_TYPE_ARRAY(LABEL_CNT_TABLE";
                    TAGLOGGER.error(msg, currTableId, StringConstant.TABLE_TYPE_LABEL_TABLE,
                            StringConstant.TABLE_TYPE_LABEL_CNT_TABLE);
                    this.gatherLabelUsersDao.updateCntTableLabelStatusFail(this.dataStatusDate, currTableId,
                            this.batchNo, "获取表状态异常，跳过处理");
                    continue;
                } else if (dstTabCntRes == 0 || usedTabCntRes == 0) {
                    // 重试不实现
                    this.gatherLabelUsersDao.updateCntTableLabelStatusFail(this.dataStatusDate, currTableId,
                            this.batchNo, "未读取到本批次宽表状态，宽表非独占，已达到最大尝试次数，跳过处理");
                    continue;
                }

                // # 修改标签状态
                TAGLOGGER.info("label_id" + TagStringUtils.trimByChar(labelIdMap.get(currTableId).toString().trim(), ","));
                this.gatherLabelUsersDao.updateTableLabelStatus(this.dataStatusDate,
                        TagStringUtils.trimByChar(labelIdMap.get(currTableId).toString().trim(), ","), this.batchNo,
                        this.pid);

                TAGLOGGER
                        .info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 3.2.2 : 创建目标表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                this.createTmpTable(currTableId, dstTable, tableDDLColMap);

                TAGLOGGER
                        .info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 3.2.2.3 插入目标表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                this.insertDataIntoTmpTable(operationTime, dstTable, srcTable, TagStringUtils.trimRightByChar(insertColMap.get(currTableId).toString().trim(), dstTable),
                        TagStringUtils.trimRightByChar(sumColStrMap.get(currTableId).toString().trim(), ","));

                TAGLOGGER.info(
                        ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 3.2.2.4 : runstats>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                this.baseDao.runstatsTable(dstTable);

                // 设置标签状态为COUNT成功
                this.gatherLabelUsersDao.updateTableLabelStatusCnSec(this.dataStatusDate, TagStringUtils.trimByChar(labelIdMap.get(currTableId).toString().trim(), ","),
                        batchNo);
                this.gatherLabelUsersDao.updateTableLabelStatusDstSec(this.dataStatusDate, currTableId, batchNo);

            }
        }
    }

    private void createTmpTable(int tableId, String tableName, Map<Integer, StringBuilder> tableDDLColMap) {
        String columnStr = "OP_TIME     DATE," + "CITY_ID     SMALLINT," + "BRAND_ID    SMALLINT,"
                + "VIP_LEVEL_ID SMALLINT"
                + TagStringUtils.trimRightByChar(tableDDLColMap.get(tableId).toString().trim(), ",");
        String distKey = "BRAND_ID,VIP_LEVEL_ID,CITY_ID";

        boolean doesTableExist = this.baseDao.doesTableExist(TagStringUtils.getScheamWithTabName(tableName),
                TagStringUtils.getTabNameWithSchema(tableName));
        if (doesTableExist) {
            this.baseDao.truncateTable(tableName);
            this.baseDao.dropTable(tableName);
        }
        baseDao.createTable(tableName, columnStr, StringConstant.TBS_DW, distKey, StringConstant.TBS_INDEX, null);
        // baseDao.createTable(tableName, columnStr, null, distKey, null, null);
    }

    private void insertDataIntoTmpTable(String opTime, String tableName, String scrTableName, String fieldName,
            String sumStr) {
        this.gatherLabelUsersDao.insertTmpTable(opTime, tableName, scrTableName, fieldName, sumStr);
    }
}
