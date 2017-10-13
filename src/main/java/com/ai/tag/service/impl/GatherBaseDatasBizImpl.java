/* Copyright(c) 2017 AsiaInfo Technologies (China), Inc.
 * All Rights Reserved.
 * FileName: GatherBaseDatasBizImpl.java
 * Author:   <a href="mailto:xiongjie3@asiainfo.com">xiongjie3</a>
 * Date:     2017年2月22日 下午4:36:01
 * Description: 汇总基础数据
 */
package com.ai.tag.service.impl;

import com.ai.tag.common.*;
import com.ai.tag.dao.IDwIndexDao;
import com.ai.tag.service.CommonService;
import com.ai.tag.utils.DateFormatUtils;
import com.ai.tag.utils.TagStringUtils;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 基础数据汇总,对应coc_d_dw_index_table_rules_yyyymmdd.tcl<br>
 *
 * @author xiongjie3
 */
@Service("gatherBaseDatasBiz")
public class GatherBaseDatasBizImpl extends BaseTaskExecution {

    private static final Logger LOGGER = LoggerFactory.getLogger(GatherBaseDatasBizImpl.class);

    @Resource
    private IDwIndexDao dwIndexDao;

    @Autowired
    private CommonService commonService;

    private String dwProductCode = "";

    private List<String> allIndexIdListStr = new ArrayList<String>();

    /*
     * (non-Javadoc)
     * @see com.ai.tag.service.IBaseTaskExecution#executeTask(com.github.ltsopensource.core.domain.Job)
     */
    @Override
    public boolean executeTask(Job job) throws TagException {

        long start = System.currentTimeMillis();
        this.getInputParams(job);
        this.init(opTime, tbCycle);
        dwProductCode = TagConstant.DAY_FLAG == this.tbCycle ? TagConstant.DW_PRODUCT_DAY_TABLE_CODE
                : TagConstant.DW_PRODUCT_MONTH_TABLE_CODE;
        // 检查重跑接口数据源参数表
        checkReRunDSParams();
        // 检查重跑指标参数
        checkReRunTagParams();
        // 确定宽表范围,主表是否准备好等
        affirmTbCode();
        // 指标处理
        dealCurrentTags();
        // 指标状态归位
        tagsRecover();
        LOGGER.info("程序完成 !! ======>coc_d_dw_index_table_rules_yyyymmdd 程序耗时{}分钟",
                (System.currentTimeMillis() - start) / 1000 / 60);
        return true;
    }

    /**
     * 功能描述: 检查重跑接口数据源参数表<br>
     */
    private void checkReRunDSParams() {
        if (!StringUtils.isEmpty(this.reRunTbCode)) {
            String srcTabList = "'" + reRunTbCode.replace(",", "','") + "'";
            StringBuilder inxCodes = new StringBuilder();
            List<String> indexCodes = dwIndexDao.getReRunDataSource(srcTabList);
            if (indexCodes.size() > 0) {
                for (int i = 0; i < indexCodes.size(); i++) {
                    if (i == 0) {
                        inxCodes.append(indexCodes.get(i));
                    } else {
                        inxCodes.append(",").append(indexCodes.get(i));
                    }
                }
            }
            reRunIndexCode = reRunIndexCode + inxCodes.toString();
        }
    }

    /**
     * 功能描述: 检查重跑指标参数<br>
     *
     * @throws TagException
     */
    private void checkReRunTagParams() throws TagException {

        if (!StringUtils.isEmpty(reRunIndexCode)) {

            String[] redoIdxList = reRunIndexCode.split(",");

            String whereRedoIdx = "'" + reRunIndexCode.replace(",", "','") + ",";

            dwIndexDao.deleteReRunTagStatus(whereRedoIdx, dataStatusDate);

            int totalTagStatus = dwIndexDao.queryReRunTagStatus(whereRedoIdx, dataStatusDate);

            if (totalTagStatus > 0) {
                LOGGER.error("从指标状态表删除重跑指标时，有指标正在被使用，删除指标列表：{}，未被清除状态指标个数：{}，日期{}", reRunIndexCode, totalTagStatus,
                        dataStatusDate);
                throw new TagException(String.format("从指标状态表删除重跑指标时，有指标正在被使用，删除指标列表：%s，未被清除状态指标个数：%s，日期%s",
                        reRunIndexCode, totalTagStatus, dataStatusDate));
            }
            StringBuilder whereDependIdxListStr = new StringBuilder("  1=0  ");
            for (String str : redoIdxList) {
                whereDependIdxListStr.append(" or T2.DEPEND_INDEX like '%").append(str).append("%'");
            }

            dwIndexDao.updateReRunTagStatus(whereDependIdxListStr.toString(), batchNo, dataStatusDate);

            dwIndexDao.selectLabeStatus(whereDependIdxListStr.toString(), dataStatusDate);

            dwIndexDao.insertReRunLabelTable(dataStatusDate, batchNo);

        }

    }

    /**
     * 功能描述:确定宽表范围 <br>
     */
    private void affirmTbCode() {
        StringBuilder tabList = new StringBuilder();
        String dealTabListStr = "";

        if (!StringUtils.isEmpty(tbCode)) {
            String[] tabListArr = tbCode.split(",");
            if (tabListArr.length > 0) {
                for (String str : tabListArr) {
                    tabList.append("'").append(str).append("',");
                }
            }
            dealTabListStr = " and T2.TARGET_TABLE_CODE in (" + tabList.toString() + "'') ";
        }

        int mainTableIsReady = dwIndexDao.queryMainTableDatas(dwProductCode, dataStatusDate);
        if (mainTableIsReady <= 0) {
            throw new TagException(String.format("没有在数据源状态表中找到本数据日期状态为%s的主表状态,不进行任何处理,主表CODE=%s,数据时间=%s",
                    StatusConstant.IDX_SRC_READY, dwProductCode, dataStatusDate));
        } else {// 插入本次要跑的指标，状态为运行中
            long pid = Thread.currentThread().getId();
            Map<String, Object> params = new HashMap<String, Object>();
            params.put("dataStatusDate", dataStatusDate);
            params.put("batchNo", batchNo);
            LOGGER.info("有主表,插入序列batch_no" + batchNo);
            params.put("threadId", pid);
            params.put("tbCycle", tbCycle);
            params.put("dateIso", dataDateIso);
            params.put("tabListStr", dealTabListStr);

            dwIndexDao.insertCurrentTags(params);
            LOGGER.info("插入数据成功，SC批次：" + params);

        }

    }

    /**
     * 功能描述:指标处理 <br>
     *
     * @throws Exception
     */
    private void dealCurrentTags() throws TagException {

        List<Map<String, Object>> currentTags = dwIndexDao.queryCurrentBatchTags(batchNo, dataStatusDate);

        if (currentTags.size() <= 0) {
            throw new TagException("没有需要处理的指标，程序退出");
        } else {

            // 目标表名
            Map<String, String> targetTableNameArr = new HashMap<String, String>();

            // 目标表周期
            Map<String, String> targetDataCycleArr = new HashMap<String, String>();

            // 目标表字段(create字段)
            Map<String, List<String>> tabColCreateArr = new HashMap<String, List<String>>();

            // 目标表字段(insert字段)
            Map<String, List<String>> insertColArr = new HashMap<String, List<String>>();

            // JOIN字段(合并表时拼字段的sql)
            Map<String, List<String>> joinColArr = new HashMap<String, List<String>>();

            // 关联表及关联条件
            Map<String, List<String>> tabJoinOnArr = new HashMap<String, List<String>>();

            // 规则条件
            Map<String, List<String>> indexColRulesArr = new HashMap<String, List<String>>();

            Map<String, Integer> srcTabIndex = new HashMap<String, Integer>();

            for (Map<String, Object> eachRow : currentTags) {

                StringBuilder tabJoinOn = new StringBuilder(" ");
                StringBuilder indexColRules = new StringBuilder(",CASE WHEN t");

                String tableCode = String.valueOf(eachRow.get("TARGET_TABLE_CODE")).trim();

                if (!srcTabIndex.containsKey(tableCode)) {
                    srcTabIndex.put(tableCode, 1);
                }

                // 目标表名

                if (!targetTableNameArr.containsKey(tableCode)) {
                    targetTableNameArr
                            .put(tableCode,
                                    String.valueOf(eachRow.get("TARGET_TABLE_NAME")).trim().substring(0,
                                            String.valueOf(eachRow.get("TARGET_TABLE_NAME")).trim().lastIndexOf("_"))
                                            + "_");
                }

                // 目标表周期
                if (!targetDataCycleArr.containsKey(tableCode)) {
                    targetDataCycleArr.put(tableCode, String.valueOf(eachRow.get("TABLE_DATA_CYCLE")).trim());
                }

                // 处理源表表名添加后缀
                String srcTabName = String.valueOf(eachRow.get("DATA_SRC_TAB_NAME")).trim().substring(0,
                        String.valueOf(eachRow.get("DATA_SRC_TAB_NAME")).trim().lastIndexOf("_")) + "_";

                String compareSrcTab = srcTabName.split(".").length == 1 ? StringConstant.DB_USER + srcTabName
                        : srcTabName;

                String compareDwMonth = TableNameConstant.DW_PRODUCT_MONTH_TABLE_NAME.split(".").length == 1
                        ? StringConstant.DB_USER + TableNameConstant.DW_PRODUCT_MONTH_TABLE_NAME
                        : TableNameConstant.DW_PRODUCT_MONTH_TABLE_NAME;

                String compareDwDay = TableNameConstant.DW_PRODUCT_DAY_TABLE_NAME.split(".").length == 1
                        ? StringConstant.DB_USER + TableNameConstant.DW_PRODUCT_DAY_TABLE_NAME
                        : TableNameConstant.DW_PRODUCT_DAY_TABLE_NAME;

                // 拼接目标表字段(create字段)
                if (tabColCreateArr.containsKey(tableCode)) {
                    tabColCreateArr.get(tableCode)
                            .add("," + eachRow.get("INDEX_CODE") + " " + eachRow.get("DATA_TYPE"));
                } else {
                    List<String> newCol = new ArrayList<String>();
                    newCol.add("," + eachRow.get("INDEX_CODE") + " " + eachRow.get("DATA_TYPE"));
                    tabColCreateArr.put(tableCode, newCol);
                }

                // 拼接目标表字段(insert字段)
                if (insertColArr.containsKey(tableCode)) {
                    insertColArr.get(tableCode).add("," + eachRow.get("INDEX_CODE") + " ");
                } else {
                    List<String> newCol = new ArrayList<String>();
                    newCol.add("," + eachRow.get("INDEX_CODE") + " ");
                    insertColArr.put(tableCode, newCol);
                }

                allIndexIdListStr.add("," + eachRow.get("INDEX_CODE"));

                // 拼接JOIN字段(合并表时拼字段的sql)
                if (joinColArr.containsKey(tableCode)) {
                    joinColArr.get(tableCode).add(",T2." + eachRow.get("INDEX_CODE") + " \n");
                } else {
                    List<String> newCol = new ArrayList<String>();
                    newCol.add(",T2." + eachRow.get("INDEX_CODE") + " \n");
                    joinColArr.put(tableCode, newCol);
                }

                // 关联表的数据周期
                String srcRunTime = "";
                int noNeedJoinFlag = 0;
                if (StringConstant.CYCLE_DEF_FT_MONTHLY == Integer
                        .valueOf(String.valueOf(eachRow.get("TABLE_DATA_CYCLE")))) {
                    try {
                        LOGGER.info("dataStatusDate" + dataStatusDate);
                        srcRunTime = DateFormatUtils
                                .dateToStr_YYYYMM(DateFormatUtils.strToDate_YYYYMM(dataStatusDate));
                        LOGGER.info("srcRunTime" + srcRunTime);
                    } catch (ParseException e) {
                        LOGGER.error("关联表的数据周期步骤时--->时间转换失败,失败原因:" + e.getMessage());
                        throw new TagException(e.getMessage());
                    }
                    noNeedJoinFlag = compareDwMonth.equalsIgnoreCase(compareSrcTab) ? 1 : 0;
                } else if (StringConstant.CYCLE_DEF_FT_DAILY == Integer
                        .valueOf(String.valueOf(eachRow.get("TABLE_DATA_CYCLE")))) {
                    srcRunTime = dataStatusDate;
                    noNeedJoinFlag = compareDwDay.equalsIgnoreCase(compareSrcTab) ? 1 : 0;
                }

                // 关联表及关联条件
                if (1 == Integer.valueOf(String.valueOf(eachRow.get("RANK")))
                        && noNeedJoinFlag == StringConstant.FLAG_FALSE) {
                    srcTabIndex.put(tableCode, srcTabIndex.get(tableCode) + 1);
                    tabJoinOn.append(eachRow.get("JOIN_RULES")).append(" ").append(srcTabName).append(srcRunTime)
                            .append(" t").append(srcTabIndex.get(tableCode)).append(" on T1.")
                            .append(StringConstant.JOIN_ID_NAME).append(" = T").append(srcTabIndex.get(tableCode))
                            .append(".").append(eachRow.get("JOIN_COLUMN")).append(" ");

                    if (tabJoinOnArr.containsKey(tableCode)) {
                        tabJoinOnArr.get(tableCode).add(tabJoinOn.toString());
                    } else {
                        List<String> tempList = new ArrayList<String>();
                        tempList.add(tabJoinOn.toString());
                        tabJoinOnArr.put(tableCode, tempList);
                    }
                }

                // 拼接规则条件
                indexColRules.append((noNeedJoinFlag == 1 ? 1 : srcTabIndex.get(tableCode))).append(".")
                        .append(eachRow.get("DATA_SRC_COL_NAME")).append(" IS NULL THEN ")
                        .append(eachRow.get("DEFAULT_VALUE")).append(" ELSE t")
                        .append((noNeedJoinFlag == 1 ? 1 : srcTabIndex.get(tableCode))).append(".")
                        .append(eachRow.get("DATA_SRC_COL_NAME")).append(" END AS ").append(eachRow.get("INDEX_CODE"));

                if (indexColRulesArr.containsKey(tableCode)) {
                    indexColRulesArr.get(tableCode).add(indexColRules.toString());
                } else {
                    List<String> newCol = new ArrayList<String>();
                    newCol.add(indexColRules.toString());
                    indexColRulesArr.put(tableCode, newCol);
                }

            }

            // table codes
            List<String> tableCodes = new ArrayList<String>();

            for (Map.Entry<String, String> tableCode : targetTableNameArr.entrySet()) {
                tableCodes.add(tableCode.getKey());
            }

            String targetRunTime = "";
            String dwProduct = "";
            String optime = "";

            for (int i = 0; i < tableCodes.size(); i++) {

                String tableCode = tableCodes.get(i);

                if (Integer.valueOf(targetDataCycleArr.get(tableCode)) == StringConstant.CYCLE_DEF_FT_MONTHLY) {

                    try {
                        targetRunTime = DateFormatUtils.dateToStr_YYYYMM(DateFormatUtils.strToDate_YYYYMM(opTime));
                        optime = DateFormatUtils
                                .dateToStr_YYYY_MM_DD(DateFormatUtils.strToDate_YYYYMMDD(opTime + "01"));
                        LOGGER.info(optime);
                    } catch (ParseException e) {
                        LOGGER.error("Step 3.3.5 : 生成本批次指标插入目标表语句--->时间转换失败,失败原因:" + e.getMessage());
                        throw new TagException(e.getMessage());
                    }
                    dwProduct = TableNameConstant.DW_PRODUCT_MONTH_TABLE_NAME + targetRunTime;
                } else {

                    try {
                        targetRunTime = DateFormatUtils.dateToStr_YYYYMMDD(DateFormatUtils.strToDate_YYYYMMDD(opTime));
                        optime = DateFormatUtils.dateToStr_YYYY_MM_DD(DateFormatUtils.strToDate_YYYYMMDD(opTime));
                        LOGGER.info(optime);
                    } catch (ParseException e) {
                        LOGGER.error("Step 3.3.5 : 生成本批次指标插入目标表语句--->时间转换失败(DAY),失败原因:" + e.getMessage());
                        throw new TagException(e.getMessage());
                    }
                    dwProduct = TableNameConstant.DW_PRODUCT_DAY_TABLE_NAME + targetRunTime;
                }

                StringBuilder joinColDdl = new StringBuilder(" ");

                StringBuilder joinColInsert = new StringBuilder();

                StringBuilder joinColSelect = new StringBuilder();

                if (StringConstant.JOIN_ID_NAME.trim().equalsIgnoreCase(StringConstant.PHONE_NO.trim())) {
                    joinColDdl.append(StringConstant.PHONE_NO).append(" ").append(StringConstant.PHONE_NO_TYPE)
                            .append(" ");
                    joinColInsert.append(StringConstant.PHONE_NO);
                    joinColSelect.append("T1.").append(StringConstant.PHONE_NO);
                } else {
                    joinColDdl.append(" ").append(StringConstant.JOIN_ID_NAME).append(" ")
                            .append(StringConstant.JOIN_ID_TYPE).append(" ").append(StringConstant.PHONE_NO).append(" ")
                            .append(StringConstant.PHONE_NO_TYPE).append(" ");

                    joinColInsert.append(StringConstant.JOIN_ID_NAME).append(" , ").append(StringConstant.PHONE_NO);
                    joinColSelect.append("T1.").append(StringConstant.JOIN_ID_NAME).append(" , T1.")
                            .append(StringConstant.PHONE_NO);
                }

                LOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 3.5 : 确定目标表字段>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                StringBuilder columnStr = new StringBuilder("op_time date,");

                columnStr.append(joinColDdl).append(",city_id ").append(StringConstant.CITY_ID_TYPE).append(",")
                        .append(StringConstant.DIM_COL_CREATE_DDL).append(TagStringUtils
                        .trimRightByChar(Joiner.on("").join(tabColCreateArr.get(tableCode)), ","));

                String distTable = StringConstant.SCHEMA_DW + "." + targetTableNameArr.get(tableCode) + targetRunTime;
                String dstTableName = targetTableNameArr.get(tableCode) + targetRunTime;

                LOGGER.info(
                        ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 3.3.5 : 生成本批次指标插入目标表语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

                StringBuilder insertSql = new StringBuilder(" (OP_TIME,");
                insertSql.append(joinColInsert).append(",CITY_ID,").append(StringConstant.DIM_COL_INSERT)
                        .append(TagStringUtils.trimRightByChar(Joiner.on("").join(insertColArr.get(tableCode)), ","))
                        .append(") select date('").append(optime).append("'),").append(joinColSelect).append(",T1.")
                        .append(StringConstant.CITY_ID_NAME).append(",").append(StringConstant.DIM_COL_SRC_TAB_SELECT)
                        .append(TagStringUtils.trimRightByChar(Joiner.on("").join(indexColRulesArr.get(tableCode)),
                                ","))
                        .append(" FROM ").append(dwProduct).append(" t1")
                        .append(Joiner.on("").join(tabJoinOnArr.get(tableCode)));

                LOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 3.6 : 查询目标表状态>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                Integer dstStatus = dwIndexDao.queryTargetTableStatu(tableCode, dataStatusDate);
                LOGGER.info("dstStatus" + " " + dstStatus);
                int dstExistsFlag = 0;
                if (dstStatus != null && StatusConstant.IDX_DST_SEC == dstStatus) {
                    dstExistsFlag = StringConstant.FLAG_TRUE;
                } else if (dstStatus == null || -1 == dstStatus) {
                    dstExistsFlag = StringConstant.FLAG_FALSE;
                } else {
                    String errorMsg = String.format("未读取到本批次宽表状态，宽表CODE=%s,状态=%s", tableCode, dstStatus);
                    LOGGER.error(errorMsg);
                    throw new TagException(errorMsg);
                }

                LOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4 : 生成表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

                String t1DstTable = StringConstant.SCHEMA_DW + ".T1_" + targetTableNameArr.get(tableCode)
                        + targetRunTime;
                String t2DstTable = StringConstant.SCHEMA_DW + ".T2_" + targetTableNameArr.get(tableCode)
                        + targetRunTime;
                String t2DstTableName = "T2_" + targetTableNameArr.get(tableCode) + targetRunTime;
                String t3DstTable = StringConstant.SCHEMA_DW + ".T3_" + targetTableNameArr.get(tableCode)
                        + targetRunTime;
                String t3DstTableName = "T3_" + targetTableNameArr.get(tableCode) + targetRunTime;

                if (StringConstant.FLAG_TRUE == dstExistsFlag && commonService.isTableExists(distTable)) {

                    LOGGER.info(
                            ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4.1 : 表存在，设置指标宽表状态为JOIN中>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                    dwIndexDao.updateDstStatus(tableCode, StringConstant.TABLE_TYPE_INDEX_TABLE,
                            StatusConstant.IDX_DST_SEC, StatusConstant.IDX_DST_RUNNING, dataStatusDate, batchNo, "0",
                            Thread.currentThread().getId(), "");

                    LOGGER.info(
                            ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4.2 : 获取本次JOIN宽表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                    int dstTabCntRes = dwIndexDao.getTargetTableStatus(tableCode, batchNo, dataStatusDate);
                    if (0 >= dstTabCntRes) {
                        updateTableIndexStatusFail("获取本次JOIN宽表状态出错", tableCode, dataStatusDate, batchNo);
                        LOGGER.error("获取本次JOIN宽表状态出错,tableCode={},dataStatusDate={},batchNo={}", tableCode,
                                dataStatusDate, batchNo);
                        throw new TagException("获取本次JOIN宽表状态出错");
                    }

                    LOGGER.info(
                            ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4.3 : 本批次指标生成到临时表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                    LOGGER.info(
                            ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4.3.1 : 创建临时表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                    commonService.createTempTable(t1DstTable, columnStr.toString(), StringConstant.TBS_TEMP,
                            StringConstant.TBS_INDEX, "0", joinColInsert.toString(), 1);

                    LOGGER.info(
                            ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4.3.2 :插入临时表 {}>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>",
                            t1DstTable);
                    dwIndexDao.insertTempTableData(t1DstTable, insertSql.toString());

                    LOGGER.info(
                            ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4.2 : 本批次指标宽表和上一次指标宽表合并，生成到临时表2>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                    LOGGER.info(
                            ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4.2.1 : 获取上一批次指标字段和类型>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

                    StringBuilder expCols = new StringBuilder("OP_TIME,");
                    expCols.append(StringConstant.JOIN_ID_NAME).append(",CITY_ID,").append(StringConstant.PHONE_NO)
                            .append(",").append(StringConstant.DIM_COL_INSERT);
                    expCols.append(Joiner.on("").join(insertColArr.get(tableCode)));

                    List<Map<String, String>> result = getColsByDictTabSqlNamespace("", StringConstant.SCHEMA_DW,
                            dstTableName, expCols.toString());

                    StringBuilder oldDstDdl = new StringBuilder("");
                    StringBuilder oldDstInsert = new StringBuilder("");
                    StringBuilder oldDstSelect = new StringBuilder("");

                    for (Map<String, String> map : result) {

                        String colName = map.get("COLNAME");
                        String colDataTypeName = map.get("FLAG01");
                        String colDataLength = String.valueOf(map.get("FLAG02"));
                        String colDataPrecision = String.valueOf(map.get("FLAG03"));

                        String colType = com.ai.tag.utils.TagStringUtils.getDataTypeByParam(colDataTypeName,
                                colDataLength, colDataPrecision);

                        oldDstDdl.append(" , ").append(colName).append(" ").append(colType).append(" \n ");

                        oldDstInsert.append(" , ").append(colName).append(" ");

                        oldDstSelect.append(" , T1.").append(colName).append(" \n ");
                    }
                    LOGGER.info(
                            ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4.2.2 : 两个批次宽表做join合并>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                    columnStr.append(oldDstDdl);
                    commonService.createTempTable(t2DstTable, columnStr.toString(), StringConstant.TBS_DW,
                            StringConstant.TBS_INDEX, "0", joinColInsert.toString(), 1);

                    StringBuilder insertDatasSql = new StringBuilder("insert into ");
                    insertDatasSql.append(t2DstTable).append(" ( OP_TIME,").append(joinColInsert).append(",CITY_ID,")
                            .append(StringConstant.DIM_COL_INSERT)
                            .append(TagStringUtils.trimRightByChar(oldDstInsert.toString().trim(), ","))
                            .append(TagStringUtils
                                    .trimRightByChar(Joiner.on("").join(insertColArr.get(tableCode)).trim(), ","))
                            .append(" ) select date('").append(optime).append("'),").append(joinColSelect)
                            .append(",T1.CITY_ID,").append(StringConstant.DIM_COL_SELECT)
                            .append(oldDstSelect.toString().trim())
                            .append(TagStringUtils.trimRightByChar(Joiner.on("").join(joinColArr.get(tableCode)).trim(),
                                    ","))
                            .append(" from ").append(distTable).append(" T1 ").append(" LEFT JOIN ").append(t1DstTable)
                            .append(" T2 on T1.").append(StringConstant.JOIN_ID_NAME).append(" = T2.")
                            .append(StringConstant.JOIN_ID_NAME);
                    LOGGER.info("insertDatasSql" + insertDatasSql);
                    this.sqlInsertNolog(dstTableName, insertDatasSql.toString());

                } else {
                    LOGGER.info(
                            ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4.1 : 表不存在，第一次生成>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

                    if (StringConstant.FLAG_TRUE == dstExistsFlag) {
                        dwIndexDao.updateDstStatus(tableCode, StringConstant.TABLE_TYPE_INDEX_TABLE,
                                StatusConstant.IDX_DST_SEC, StatusConstant.IDX_DST_RUNNING, dataStatusDate, batchNo,
                                StringConstant.FLAG_TRUE + "", Thread.currentThread().getId(), "");
                    } else {
                        dwIndexDao.insertDstStatus(tableCode, StringConstant.TABLE_TYPE_INDEX_TABLE,
                                StatusConstant.IDX_DST_RUNNING, dataStatusDate, batchNo,
                                Thread.currentThread().getId());
                    }

                    LOGGER.info(
                            ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4.1.1 : 创建目标表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                    try {
                        //用columnStr直接创建新的T2，表不存在时就不需要经过T1与上一批次的合并为T2；
                        //而表存在时T1生成后与上一批次表合并生成T2,把已存在的表rename成T3，把T2rename成目标表，在删除T1/T3完成
                        commonService.createTempTable(t2DstTable, columnStr.toString(), StringConstant.TBS_DW,
                                StringConstant.TBS_INDEX, "", joinColInsert.toString(), 1);
                    } catch (Exception e) {
                        String errorMsg = String.format("创建表失败，失败原因%s", e.getMessage());
                        updateTableIndexStatusFail(errorMsg, tableCode, dataStatusDate, batchNo);
                        LOGGER.error(errorMsg);
                        throw new TagException(errorMsg);
                    }

                    LOGGER.info(
                            ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4.1.2 :插入目标表 {}>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>",
                            t2DstTable);
                    StringBuilder insertDatasSql = new StringBuilder("insert into ");
                    insertDatasSql.append(t2DstTable).append(" ").append(insertSql.toString());
                    this.sqlInsertNolog(t2DstTable, insertDatasSql.toString());
                }

                LOGGER.info(
                        ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4.3 : rename目标表为临时表3-->T3_目标表，rename 临时表2为目标表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                if (commonService.isTableExists(distTable)) {
                    baseDao.renameTable(StringConstant.SCHEMA_DW, dstTableName, t3DstTableName);
                }
                baseDao.renameTable(StringConstant.SCHEMA_DW, t2DstTableName, dstTableName);

                LOGGER.info(
                        ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4.4 : drop临时表1，临时表3...T1={},T3={}>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>",
                        t1DstTable, t3DstTable);
                if (dstExistsFlag == StringConstant.FLAG_TRUE && commonService.isTableExists(dstTableName)) {

                    baseDao.truncateTable(t1DstTable);
                    baseDao.dropTable(t1DstTable);
                }

                if (commonService.isTableExists(t3DstTable)) {
                    baseDao.truncateTable(t3DstTable);
                    baseDao.dropTable(t3DstTable);
                }

                LOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 3.5.2 :建立索引>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                baseDao.createIndex(distTable + "_I", distTable, joinColInsert.toString());

                // #更新目标表统计信息
                baseDao.runstatsTable(distTable);

                LOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4.5 : 修改指标状态>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                String whereIdxList = TagStringUtils
                        .trimRightByChar(Joiner.on("").join(insertColArr.get(tableCode)).trim(), ",")
                        .replaceAll(",", "','").replaceAll(" ", "");

                dwIndexDao.updateTagsStatus(StatusConstant.INDEX_SEC, StatusConstant.INDEX_RUNNING, dataStatusDate,
                        batchNo, whereIdxList);
                LOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4.6 : 修改目标表状态>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                dwIndexDao.updateDstStatus(tableCode, StringConstant.TABLE_TYPE_INDEX_TABLE,
                        StatusConstant.IDX_DST_RUNNING, StatusConstant.IDX_DST_SEC, dataStatusDate, batchNo,
                        StringConstant.FLAG_TRUE + "", Thread.currentThread().getId(), "");

            }

        }
    }

    /**
     * 功能描述:做表操作失败时，更新该宽表所有正在处理标签状态为失败 <br>
     *
     * @param errorMsg
     * @param tableId
     * @param dataStatusDate
     * @param currBatch
     * @throws TagException
     */
    private void updateTableIndexStatusFail(String errorMsg, String tableId, String dataStatusDate, long currBatch)
            throws TagException {
        dwIndexDao.updateTableIndexStatusFail(errorMsg, tableId, dataStatusDate, currBatch);
    }

    /**
     * 功能描述:根据表类型从字典表中获取表列信息 <br>
     *
     * @param sqlNamesapce 数据库连接所在命名空间
     * @param tableSchema  表所在的schema
     * @param tableName    表名
     * @param expClos      不希望出现在结果集中的列名
     * @return
     * @throws TagException
     * @see [相关类/方法](可选)
     * @since [产品/模块版本](可选)
     */
    private List<Map<String, String>> getColsByDictTabSqlNamespace(String sqlNamesapce, String tableSchema,
                                                                   String tableName, String expClos) throws TagException {

        String[] tableNames = tableName.split("\\.");

        String schema = "".equals(tableSchema) && tableNames.length == 2 ? tableNames[0].toUpperCase()
                : tableSchema.toUpperCase();

        String tabName;
        if (tableNames.length == 2) {
            tabName = tableNames[1].toUpperCase();
        } else {
            tabName = tableNames[0].toUpperCase();
        }

        StringBuilder expColsSql = new StringBuilder("1=1 ");

        expClos = expClos.replaceAll(" ", "");
        for (String str : expClos.split(",")) {
            expColsSql.append(" and COLNAME<>").append("'" + str.toUpperCase() + "'");
        }

        return dwIndexDao.getColsByDictTabSqlNamespace(schema, tabName, expColsSql.toString());
    }

    /**
     * 功能描述:无日志写入<br>
     *
     * @param dstTable
     * @param sql
     * @throws TagException
     */
    private void sqlInsertNolog(String dstTable, String sql) throws TagException {
        String[] tableNames = dstTable.split("\\.");

        String table = tableNames.length == 2 ? tableNames[1] : tableNames[0];
        String schema = tableNames.length == 2 ? tableNames[0] : StringConstant.SCHEMA_DW;
        baseDao.dbDisableLog(schema + "." + table);
        dwIndexDao.insertDstTableData(sql);

    }

    /**
     * 功能描述: <br>
     */
    private void tagsRecover() {

        dwIndexDao.recoverTagStatusOk(dataStatusDate, batchNo,
                TagStringUtils.trimRightByChar(Joiner.on("").join(allIndexIdListStr).trim(), ",").replaceAll(",", "','")
                        .replaceAll(" ", ""));
    }

}
