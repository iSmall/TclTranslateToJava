/* Copyright(c) 2017 AsiaInfo Technologies (China), Inc.
 * All Rights Reserved.
 * FileName: DealTheLableDatasServiceImpl.java
 * Author:   <a href="mailto:xiongjie3@asiainfo.com">xiongjie3</a>
 * Date:     2017年3月2日 下午1:55:55
 * Description: 通过标签配置表和标签规则表和指标配置表，从指标宽表生成标签宽表      
 */
package com.ai.tag.service.impl;

import com.ai.tag.common.*;
import com.ai.tag.dao.IDealLabelDatasDao;
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
 * coc 标签层 数据处理,对应coc_d_dw_label_table_rules_yyyymmdd.tcl<br>
 * 通过标签配置表和标签规则表和指标配置表，从指标宽表生成标签宽表</br>
 * 标签配置表：</br>
 * CI_APPROVE_STATUS</br>
 * CI_LABEL_EXT_INFO</br>
 * CI_LABEL_INFO</br>
 * CI_MDA_SYS_TABLE</br>
 * CI_MDA_SYS_TABLE_COLUMN</br>
 * DIM_COLUMN_DATA_TYPE</br>
 * 标签规则配置表：</br>
 * DIM_COC_LABEL_COUNT_RULES</br>
 * 指标配置表：</br>
 * DIM_COC_INDEX_MODEL_TABLE_CONF</br>
 * DIM_COC_INDEX_INFO</br>
 *
 * @author xiongjie3
 */

@Service("dealLableService")
public class DealLableDatasServiceImpl extends BaseTaskExecution {

    private static final Logger LOGGER = LoggerFactory.getLogger(DealLableDatasServiceImpl.class);

    @Resource
    private IDealLabelDatasDao lableDao;

    @Resource
    private IDwIndexDao indexDao;

    @Autowired
    private CommonService commonService;

    private String updateNewstDateStr;

    /*
     * (non-Javadoc)
     * @see com.ai.tag.service.IBaseTaskExecution#executeTask(com.github.ltsopensource.core.domain.Job)
     */
    @Override
    public boolean executeTask(Job job) throws TagException {

        long start = System.currentTimeMillis();

        this.getInputParams(job);
        this.init(opTime, tbCycle);
        //预处理
        LOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 1.1 : 预处理日期表状态>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        if (TagConstant.DAY_FLAG == tbCycle) {
            this.updateNewstDateStr = "UPDATE  " + TableNameConstant.CI_NEWEST_LABEL_DATE
                    + " SET DAY_NEWEST_DATE ='" + this.opTime + "',DAY_NEWEST_STATUS=0 where DAY_NEWEST_DATE<='"
                    + this.opTime + "'";
        } else if (TagConstant.MONTH_FLAG == tbCycle) {
            this.updateNewstDateStr = "UPDATE " + TableNameConstant.CI_NEWEST_LABEL_DATE
                    + " SET MONTH_NEWEST_DATE ='" + opTime
                    + "',MONTH_NEWEST_STATUS =0 where MONTH_NEWEST_DATE<='" + opTime + "'";
        }
        this.lableDao.updateDataCycle(this.updateNewstDateStr);

        // 1.宽表范围确定
        StringBuilder tabListStr = new StringBuilder("");
        if (!StringUtils.isEmpty(tbCode)) {
            tabListStr.append(" and T1.TABLE_ID in (");
            StringBuilder tabList = new StringBuilder();
            for (String tableCode : tbCode.split(",")) {
                tabList.append(" ").append(tableCode).append(",");
            }
            tabListStr.append(TagStringUtils.trimRightByChar(tabList.toString(), ",")).append(") ");
        }

        // 2.获取批次
        if (0 >= batchNo) {
            throw new TagException("取当前程序批次中，获取sequence为空");
        }

        LOGGER.info(
                ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 2 : 从 DIM_COC_LABEL_TABLE 中取出符合条件的表名名称、周期等，放入数组中>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        List<Map<String, Object>> labelSqlResult = lableDao.getTagsList(dataStatusDate, dataDateIso,
                tabListStr.toString(), tbCycle);

        if (labelSqlResult.size() <= 0) {
            throw new TagException("取标签失败");
        }

        Map<String, String> tableSchemaArr = new HashMap<String, String>();

        Map<String, String> tableNameArr = new HashMap<String, String>();

        Map<String, List<String>> tableDdlColArr = new HashMap<String, List<String>>();

        Map<String, List<String>> insertColArr = new HashMap<String, List<String>>();

        Map<String, List<String>> joinColArr = new HashMap<String, List<String>>();

        Map<String, List<String>> labelIdListStrArr = new HashMap<String, List<String>>();

        Map<String, List<String>> rulesColArr = new HashMap<String, List<String>>();

        Map<String, Integer> updateLabelDataStateId = new HashMap<String, Integer>();

        Map<String, List<String>> whereColArr = new HashMap<String, List<String>>();

        Map<String, List<String>> updateStateLabelListArray = new HashMap<String, List<String>>();

        LOGGER.info(
                ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 3 : loop每张宽表，进行处理...循环一开始>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

        LOGGER.info(
                ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 3.1 : 获取每个表的表信息,label信息,字段信息,rules信息...label个数:{}>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>",
                labelSqlResult.size());
        for (Map<String, Object> eachRow : labelSqlResult) {

            String tableId = String.valueOf(eachRow.get("TABLE_ID")).trim();
            String countRules = String.valueOf(eachRow.get("COUNT_RULES")).trim();

            tableNameArr.put(tableId, String.valueOf(eachRow.get("TABLE_NAME")));

            // # schema名
            // # 默认schema dw
            if (Integer.valueOf(String.valueOf(eachRow.get("TABLE_SCHEMA")).trim()) != StringConstant.FLAG_NO_NULL) {

                if (!tableSchemaArr.containsKey(tableId))
                    tableSchemaArr.put(tableId, String.valueOf(eachRow.get("TABLE_SCHEMA")));

            } else {
                if (!tableSchemaArr.containsKey(tableId)) {
                    tableSchemaArr.put(tableId, StringConstant.SCHEMA_DW);
                }
            }

            // 处理字段名
            // # 0/1签，父规则为R_00000，父节点的规则由其子节点的规则合并(取或)而来
            StringBuilder treeDependIndex = new StringBuilder();
            if (StringConstant.RULE_CODE_PARENT.equals(countRules) && StringConstant.LABEL_TYPE_ONE_DIM == Integer
                    .valueOf(String.valueOf(eachRow.get("LABEL_TYPE_ID")).trim())) {

                LOGGER.info(
                        ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 3.1.3 : 父节点处理...label_id={}>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>",
                        eachRow.get("LABEL_ID"));

                LOGGER.info(
                        ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 3.1.3.1 : 获取所有子节点>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                List<Map<String, Object>> sonTagsRules = lableDao
                        .getSonTagsRules(String.valueOf(eachRow.get("LABEL_ID")).trim(), dataDateIso);

                StringBuilder parentRules = new StringBuilder();
                if (sonTagsRules.size() >= 1) {

                    for (Map<String, Object> sonTagRule : sonTagsRules) {
                        treeDependIndex.append(String.valueOf(sonTagRule.get("DEPEND_INDEX"))).append(",");
                        parentRules.append(" OR ").append("(").append(countRules).append(")");
                    }

                }
                treeDependIndex = new StringBuilder();
                treeDependIndex.append(TagStringUtils.trimByChar(treeDependIndex.toString(), ","));

                countRules = " 1=0 " + parentRules.toString();

            }

            String labelDependIndexList = TagStringUtils
                    .trimByChar((String.valueOf(eachRow.get("DEPEND_INDEX")) + "," + treeDependIndex.toString())
                            .replaceAll("\"", "").trim(), ",");

            String[] forList = labelDependIndexList.split(",");

            StringBuilder tempIdxList = new StringBuilder();
            int dependIdxCnt = 0;
            for (String str : forList) {

                if ((StringConstant.FLAG_NO_NULL + "").equals(str)
                        || (!"".equals(tempIdxList.toString()) && tempIdxList.toString().contains(str))) {
                    continue;
                } else {
                    tempIdxList.append(str);
                    dependIdxCnt++;
                }

            }

            if (dependIdxCnt == 0) {
                continue;
            }

            labelDependIndexList = labelDependIndexList.replaceAll(",", "','");

            if (StatusConstant.LABEL_WAIT_RERUN == Integer.valueOf(String.valueOf(eachRow.get("DATA_STATUS")))) {

                lableDao.updateTagStatusTable(batchNo, Thread.currentThread().getId(),
                        Long.valueOf(String.valueOf(eachRow.get("LABEL_ID"))), dataStatusDate, dependIdxCnt,
                        labelDependIndexList);
            } else {
                lableDao.insertTagStatusTable(batchNo, Thread.currentThread().getId(),
                        Long.valueOf(String.valueOf(eachRow.get("LABEL_ID"))), dataStatusDate, dependIdxCnt,
                        labelDependIndexList);
            }

            // 从标签状态表读标签状态，如果没获取到或者获取失败，continue，处理下一个标签

            if (lableDao.getTotalTagsStatus(Long.valueOf(String.valueOf(eachRow.get("LABEL_ID"))), dataStatusDate,
                    batchNo) <= 0) {
                LOGGER.info("标签不在本批次处理列表中，跳过处理...label_id={}", eachRow.get("LABEL_ID"));
                continue;
            }

            if ((StringConstant.FLAG_NO_NULL + "").equals(String.valueOf(eachRow.get("DATA_TYPE")))
                    || "".equals(String.valueOf(eachRow.get("DATA_TYPE")))) {
                String errorMsg = String.format(
                        "字段类型配置异常,请排查异常数据后从标签状态表删除本批次状态重跑,请检查字段类型配置,label_id=%s,column_name=%s,DATA_TYPE=%s",
                        eachRow.get("TABLE_ID"), eachRow.get("COLUMN_NAME"), eachRow.get("DATA_TYPE"));
                LOGGER.error(errorMsg);
                updateLabelStatusFail(errorMsg, dataStatusDate, batchNo, String.valueOf(eachRow.get("LABEL_ID")));
                throw new TagException(errorMsg);
            }

            // # 0/1签和属性签规则字段不存在，处理下一个标签
            if (String.valueOf(StringConstant.FLAG_NO_NULL).equals(countRules) && !String
                    .valueOf(StringConstant.LABEL_TYPE_ATTR).equals(String.valueOf(eachRow.get("LABEL_TYPE_ID")))) {

                String errorMsg = String.format("标签规则字段不存在，请检查: COUNT_RULES_CODE =%s label_id=%s",
                        String.valueOf(eachRow.get("COUNT_RULES_CODE")), String.valueOf(eachRow.get("LABEL_TYPE_ID")));

                LOGGER.error(errorMsg);
                updateLabelStatusFail(errorMsg, dataStatusDate, batchNo, String.valueOf(eachRow.get("LABEL_ID")));
                throw new TagException(errorMsg);
            }

            if (String.valueOf(StringConstant.FLAG_NO_NULL).equals(countRules)
                    && Integer.valueOf(String.valueOf(eachRow.get("LABEL_TYPE_ID"))) != StringConstant.LABEL_TYPE_ATTR
                    && !StringConstant.RULE_CODE_PARENT.equals(String.valueOf(eachRow.get("COUNT_RULES_CODE")))) {
                String errorMsg = String.format("[Warning]非属性签和R0000规则标签没有具体规则，请检查： COUNT_RULES_CODE = %s",
                        eachRow.get("COUNT_RULES_CODE"));
                LOGGER.error(errorMsg);
                updateLabelStatusFail(errorMsg, dataStatusDate, batchNo, String.valueOf(eachRow.get("LABEL_ID")));
                throw new TagException(errorMsg);
            }

            // # 处理规则
            if (!String.valueOf(StringConstant.FLAG_NO_NULL).equals(String.valueOf(eachRow.get("DEPEND_INDEX")))
                    && !StringUtils.isEmpty(eachRow.get("DEPEND_INDEX"))) {

                if (whereColArr.containsKey(tableId)) {
                    whereColArr.get(tableId).add(String.valueOf(eachRow.get("DEPEND_INDEX")) + ",");
                } else {
                    List<String> temp = new ArrayList<String>();
                    temp.add(String.valueOf(eachRow.get("DEPEND_INDEX")) + ",");
                    whereColArr.put(tableId, temp);
                }

            } else if (!StringUtils.isEmpty(eachRow.get("DEPEND_INDEX"))) {
                if (whereColArr.containsKey(tableId)) {
                    whereColArr.get(tableId).add(treeDependIndex.toString() + ",");
                } else {
                    List<String> temp = new ArrayList<String>();
                    temp.add(treeDependIndex.toString() + ",");
                    whereColArr.put(tableId, temp);
                }
            }

            // 存在标签规则时，拼字段信息和select信息

            if (tableDdlColArr.containsKey(tableId)) {
                tableDdlColArr.get(tableId).add("," + String.valueOf(eachRow.get("COLUMN_NAME")) + " "
                        + String.valueOf(eachRow.get("DATA_TYPE")) + " ");
            } else {
                List<String> temp = new ArrayList<String>();
                temp.add("," + String.valueOf(eachRow.get("COLUMN_NAME")) + " "
                        + String.valueOf(eachRow.get("DATA_TYPE")) + " ");
                tableDdlColArr.put(tableId, temp);
            }

            if (insertColArr.containsKey(tableId)) {
                insertColArr.get(tableId).add("," + String.valueOf(eachRow.get("COLUMN_NAME")));
            } else {
                List<String> temp = new ArrayList<String>();
                temp.add("," + String.valueOf(eachRow.get("COLUMN_NAME")));
                insertColArr.put(tableId, temp);
            }

            if (joinColArr.containsKey(tableId)) {
                joinColArr.get(tableId).add(",T2." + String.valueOf(eachRow.get("COLUMN_NAME")));
            } else {
                List<String> temp = new ArrayList<String>();
                temp.add(",T2." + String.valueOf(eachRow.get("COLUMN_NAME")));
                joinColArr.put(tableId, temp);
            }

            if (labelIdListStrArr.containsKey(tableId)) {
                labelIdListStrArr.get(tableId).add("," + String.valueOf(eachRow.get("LABEL_ID")));
            } else {
                List<String> temp = new ArrayList<String>();
                temp.add("," + String.valueOf(eachRow.get("LABEL_ID")));
                labelIdListStrArr.put(tableId, temp);
            }

            // 根据标签类型拼语句
            if (Integer.valueOf(String.valueOf(eachRow.get("LABEL_TYPE_ID"))) == StringConstant.LABEL_TYPE_ONE_DIM) {

                if (rulesColArr.containsKey(tableId)) {
                    rulesColArr.get(tableId).add(",case when " + countRules + " then 1 else 0 end "
                            + String.valueOf(eachRow.get("COLUMN_NAME")) + " ");
                } else {
                    List<String> temp = new ArrayList<String>();
                    temp.add(",case when " + countRules + " then 1 else 0 end "
                            + String.valueOf(eachRow.get("COLUMN_NAME")) + " ");
                    rulesColArr.put(tableId, temp);
                }

            } else if (Integer
                    .valueOf(String.valueOf(eachRow.get("LABEL_TYPE_ID"))) == StringConstant.LABEL_TYPE_ATTR) {

                if (rulesColArr.containsKey(tableId)) {
                    rulesColArr.get(tableId).add("," + String.valueOf(eachRow.get("DEPEND_INDEX")) + "  "
                            + String.valueOf(eachRow.get("COLUMN_NAME")) + " \n");
                } else {
                    List<String> temp = new ArrayList<String>();
                    temp.add("," + String.valueOf(eachRow.get("DEPEND_INDEX")) + "  "
                            + String.valueOf(eachRow.get("COLUMN_NAME")) + " \n");
                    rulesColArr.put(tableId, temp);
                }

                // # 这里先认为属性签的虚拟父签在这一步完成处理的

                if (String.valueOf(eachRow.get("DATA_STATUS_ID")).equals(StringConstant.DATA_STATUS_NOT_EFFECT)) {
                    updateLabelDataStateId.put(tableId, StringConstant.FLAG_TRUE);
                }

                if (updateStateLabelListArray.containsKey(tableId)) {
                    updateStateLabelListArray.get(tableId).add(String.valueOf(eachRow.get("LABEL_ID")));
                } else {
                    List<String> temp = new ArrayList<String>();
                    temp.add(String.valueOf(eachRow.get("LABEL_ID")));
                    updateStateLabelListArray.put(tableId, temp);
                }

            } else {// # 其他类型标签，规则为计算规则，在规则跑完处理流程就结束，添加到要更新状态的标签中

                if (rulesColArr.containsKey(tableId)) {
                    rulesColArr.get(tableId)
                            .add("," + countRules + "  " + String.valueOf(eachRow.get("COLUMN_NAME")) + " \n");
                } else {
                    List<String> temp = new ArrayList<String>();
                    temp.add("," + countRules + "  " + String.valueOf(eachRow.get("COLUMN_NAME")) + " \n");
                    rulesColArr.put(tableId, temp);
                }

                if (String.valueOf(eachRow.get("DATA_STATUS_ID")).equals(StringConstant.DATA_STATUS_NOT_EFFECT)) {
                    updateLabelDataStateId.put(tableId, StringConstant.FLAG_TRUE);
                }

                if (updateStateLabelListArray.containsKey(tableId)) {
                    updateStateLabelListArray.get(tableId).add(String.valueOf(eachRow.get("LABEL_ID")));
                } else {
                    List<String> temp = new ArrayList<String>();
                    temp.add(String.valueOf(eachRow.get("LABEL_ID")));
                    updateStateLabelListArray.put(tableId, temp);
                }

            }

        }

        if (tableDdlColArr.size() <= 0) {
            LOGGER.info("本批次没有待处理标签...程序退出");
            throw new TagException("本批次没有待处理标签...程序退出");
        }

        // 循环三开始

        List<String> updateDstFailList = new ArrayList<String>();

        Map<String, List<String>> failMsgArr = new HashMap<String, List<String>>();

        Map<String, Integer> tabInd = new HashMap<String, Integer>();

        for (Map.Entry<String, String> tableNameMap : tableNameArr.entrySet()) {

            String tableId = tableNameMap.getKey();

            if (!tabInd.containsKey(tableId)) {
                tabInd.put(tableId, 1);
            }

            if (!tableDdlColArr.containsKey(tableId)) {
                LOGGER.info("table中没有标签字段,table_id={}", tableId);
                continue;
            }

            // #******************************************************************
            // # 将where条件中的字符串增加引号
            // #******************************************************************
            StringBuilder whereCols = new StringBuilder("");

            whereCols.append("'" + Joiner.on("").join(whereColArr.get(tableId)).replaceAll(",", "','") + "'''");

            // 置状态失败要回滚状态为SEC
            lableDao.recoverTagsStataus(batchNo);

            LOGGER.info(
                    ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>3.3 从 DIM_COC_INDEX_MODEL_TABLE_CONF 、 DIM_COC_INDEX_INFO 中取出BI数据源表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

            StringBuilder joinCol = new StringBuilder();

            StringBuilder usedIdxTableSql = new StringBuilder(" 1=0 ");

            List<Map<String, Object>> srcTables = lableDao.getSrcTableByTagRules(whereCols.toString(), dataDateIso);

            for (Map<String, Object> srcTable : srcTables) {

                String srcRunTime = "";

                String idxTargetTableCode = String.valueOf(srcTable.get("TARGET_TABLE_CODE"));
                String idxTargetTableName = String.valueOf(srcTable.get("TARGET_TABLE_NAME"));
                int idxTableDataCycle = Integer.valueOf(String.valueOf(srcTable.get("TABLE_DATA_CYCLE")));
                int joinTableNo = Integer.valueOf(String.valueOf(srcTable.get("RANK")));

                // # 处理指标源表后缀
                idxTargetTableName = idxTargetTableName.trim().substring(0, idxTargetTableName.lastIndexOf("_")) + "_";

                // # 根据统计周期确定源表时间后缀
                if (StringConstant.CYCLE_DEF_FT_MONTHLY == idxTableDataCycle) {
                    try {
                        srcRunTime = DateFormatUtils.dateToStr_YYYYMM(DateFormatUtils.strToDate_YYYYMM(opTime));
                    } catch (ParseException e) {
                        LOGGER.error("根据统计周期确定源表时间后缀(Month):" + e.getMessage());
                        throw new TagException(e.getMessage());
                    }
                } else if (StringConstant.CYCLE_DEF_FT_DAILY == idxTableDataCycle) {

                    if (tbCycle == StringConstant.CYCLE_DEF_FT_MONTHLY) {
                        try {
                            srcRunTime = DateFormatUtils
                                    .getCurrentMonthLastDayOfYYYYMMDD(DateFormatUtils.strToDate_YYYYMM(opTime));
                        } catch (ParseException e) {
                            LOGGER.error("根据统计周期确定源表时间后缀(Day):" + e.getMessage());
                            throw new TagException(e.getMessage());
                        }
                    } else {
                        srcRunTime = opTime;
                    }

                }

                usedIdxTableSql.append(" or (TABLE_ID='").append(idxTargetTableCode).append("' and DATA_DATE='")
                        .append(srcRunTime).append("')");

                if (1 == joinTableNo) {
                    tabInd.put(tableId, tabInd.get(tableId) + 1);
                    joinCol.append("  LEFT JOIN ").append(StringConstant.SCHEMA_DW).append(".")
                            .append(idxTargetTableName).append(srcRunTime).append(" t").append(tabInd.get(tableId))
                            .append(" ON t1.").append(StringConstant.JOIN_ID_NAME).append("=t")
                            .append(tabInd.get(tableId)).append(".").append(StringConstant.JOIN_ID_NAME);
                }

            }

            try {
                lableDao.updateTagsStatus(batchNo, Thread.currentThread().getId(), usedIdxTableSql.toString());
            } catch (Exception e) {

                String errorMsg = "更新指标表状态";
                updateDstFailList.add(tableId);

                if (failMsgArr.containsKey(tableId)) {
                    failMsgArr.get(tableId).add(errorMsg);
                } else {
                    List<String> temp = new ArrayList<String>();
                    temp.add(errorMsg);
                    failMsgArr.put(tableId, temp);
                }

                updateTableLabelStatusFail(errorMsg, tableId, dataStatusDate, batchNo);
                LOGGER.error(errorMsg);
                throw new TagException(errorMsg);

            }

            // int dstTabCntRes = 0;
            // try {
            // dstTabCntRes = lableDao.getTablesStatus(batchNo, usedIdxTableSql.toString());
            // } catch (Exception e) {
            // String errorMsg = "获取${SRC_TABLE_ARRAY(DST_TABLE_STATUS)}表状态失败";
            // LOGGER.error("errorMsg");
            // updateTableLabelStatusFail(errorMsg, tableId, dataStatusDate, batchNo);
            // throw new TagException(errorMsg);
            // }
            LOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>3.3.2 确定目标表名>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            String targetRunTime = "";
            String dwProduct = "";
            String optime = "";
            if (tbCycle == StringConstant.CYCLE_DEF_FT_MONTHLY) {
                try {
                    targetRunTime = DateFormatUtils.dateToStr_YYYYMM(DateFormatUtils.strToDate_YYYYMM(opTime));
                } catch (ParseException e) {
                    LOGGER.error("3.3.2 确定目标表名--->targetRunTime时间转换失败(Month):" + e.getMessage());
                    throw new TagException(e.getMessage());
                }

                dwProduct = StringConstant.DW_PRODUCT_MONTH_TABLE_NAME + targetRunTime;
                try {
                    optime = DateFormatUtils
                            .dateToStr_YYYY_MM_DD(DateFormatUtils.strToDate_YYYYMMDD(targetRunTime + "01"));
                } catch (ParseException e) {
                    LOGGER.error("3.3.2 确定目标表名--->targetRunTime时间转换失败(Month):" + e.getMessage());
                    throw new TagException(e.getMessage());
                }

            } else {
                targetRunTime = opTime;
                dwProduct = StringConstant.DW_PRODUCT_DAY_TABLE_NAME + targetRunTime;
                try {
                    optime = DateFormatUtils.dateToStr_YYYY_MM_DD(DateFormatUtils.strToDate_YYYYMMDD(opTime));
                } catch (ParseException e) {
                    LOGGER.error("3.3.2 确定目标表名--->targetRunTime时间转换失败(Day):" + e.getMessage());
                    throw new TagException(e.getMessage());
                }
            }

            LOGGER.info(
                    ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>3.3.3 创建目标表，插入数据，建立索引，runstats>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

            StringBuilder joinColDdl = new StringBuilder();

            StringBuilder joinColInsert = new StringBuilder();

            StringBuilder joinColSelect = new StringBuilder();

            StringBuilder columnStr = new StringBuilder();

            StringBuilder distKey = new StringBuilder();

            StringBuilder dstTable = new StringBuilder();

            StringBuilder dstTableName = new StringBuilder();

            if (StringConstant.JOIN_ID_NAME.trim().equalsIgnoreCase(StringConstant.PHONE_NO.trim())) {
                joinColDdl.append(" ").append(StringConstant.PHONE_NO).append(" ").append(StringConstant.PHONE_NO_TYPE)
                        .append(" ");
                joinColInsert.append(StringConstant.PHONE_NO);
                joinColSelect.append("T1.").append(StringConstant.PHONE_NO);

            } else {

                joinColDdl.append(" ").append(StringConstant.JOIN_ID_NAME).append(" ")
                        .append(StringConstant.JOIN_ID_TYPE).append(",").append(StringConstant.PHONE_NO).append(" ")
                        .append(StringConstant.PHONE_NO_TYPE).append(" ");
                joinColInsert.append(StringConstant.JOIN_ID_NAME).append(" , ").append(StringConstant.PHONE_NO);
                joinColSelect.append(" ").append("T1.").append(StringConstant.JOIN_ID_NAME).append(" ,").append("T1.")
                        .append(StringConstant.PHONE_NO);

            }

            LOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 3.3.3 : 确定目标表字段>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

            columnStr.append("op_time date,").append(joinColDdl).append(",").append("CITY_ID ")
                    .append(StringConstant.CITY_ID_TYPE).append(" ,").append(StringConstant.DIM_COL_CREATE_DDL)
                    .append(TagStringUtils.trimRightByChar(Joiner.on("").join(tableDdlColArr.get(tableId)), ","));

            distKey.append(joinColInsert);
            dstTable.append(tableSchemaArr.get(tableId)).append(".").append(tableNameArr.get(tableId)).append("_")
                    .append(targetRunTime);
            dstTableName.append(tableNameArr.get(tableId)).append("_").append(targetRunTime);

            LOGGER.info(
                    ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 3.3.5 : 生成本批次标签插入目标表语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            int dstExistsFlag = 0;
            Integer dstStatus = lableDao.getTargetStatus(tableId, dataStatusDate);
            if (dstStatus != null && dstStatus == StatusConstant.LABEL_DST_SEC) {
                dstExistsFlag = StringConstant.FLAG_TRUE;
            } else if (dstStatus != null && dstStatus == -1) {
                dstExistsFlag = StringConstant.FLAG_FALSE;
            }

            LOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4 : 生成表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            String t1DstTable = tableSchemaArr.get(tableId) + ".T1_" + tableNameArr.get(tableId) + "_" + targetRunTime;
            String t2DstTable = tableSchemaArr.get(tableId) + ".T2_" + tableNameArr.get(tableId) + "_" + targetRunTime;
            String t2DstTableName = "T2_" + tableNameArr.get(tableId) + "_" + targetRunTime;
            String t3DstTable = tableSchemaArr.get(tableId) + ".T3_" + tableNameArr.get(tableId) + "_" + targetRunTime;
            String t3DstTableName = "T3_" + tableNameArr.get(tableId) + "_" + targetRunTime;

            StringBuilder insertSql = new StringBuilder();

            insertSql.append("(op_time,").append(joinColInsert).append(",city_id,")
                    .append(StringConstant.DIM_COL_INSERT)
                    .append(TagStringUtils.trimRightByChar(Joiner.on("").join(insertColArr.get(tableId)).trim(), ","))
                    .append(") select date('").append(optime).append("'),").append(joinColSelect).append(",T1.")
                    .append(StringConstant.CITY_ID_NAME).append(",").append(StringConstant.DIM_COL_SRC_TAB_SELECT)
                    .append(TagStringUtils.trimRightByChar(Joiner.on("").join(rulesColArr.get(tableId)).trim(), ","))
                    .append(" FROM ").append(dwProduct).append(" t1 ").append(joinCol);

            if (dstExistsFlag == StringConstant.FLAG_TRUE && commonService.isTableExists(dstTable.toString())) {

                LOGGER.info(
                        ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4.1 : 表存在，设置标签宽表状态为JOIN中>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                try {
                    indexDao.updateDstStatus(tableId, StringConstant.TABLE_TYPE_LABEL_TABLE,
                            StatusConstant.LABEL_DST_SEC, StatusConstant.LABEL_DST_RUNNING, dataStatusDate, batchNo,
                            String.valueOf(StringConstant.FLAG_FALSE), Thread.currentThread().getId(), "");
                } catch (Exception e) {

                    String errorMsg = "插入表状态中失败";
                    updateDstFailList.add(tableId);
                    updateTableLabelStatusFail(errorMsg, tableId, dataStatusDate, batchNo);
                    throw new TagException(errorMsg);

                }

                LOGGER.info(
                        ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4.2 : 获取本次JOIN宽表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                if (lableDao.getCurrentWidthTable(tableId, batchNo, dataStatusDate) < 0) {
                    continue;
                }

                LOGGER.info(
                        ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4.3 : 本批次标签生成到临时表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                LOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4.3.1 : 创建临时表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                try {
                    commonService.createTempTable(t1DstTable, columnStr.toString(), StringConstant.TBS_TEMP,
                            StringConstant.TBS_INDEX, "0", distKey.toString(), 1);
                } catch (Exception e) {
                    String errorMsg = "本批次标签生成到临时表中，创建表 失败";

                    updateDstFailList.add(tableId);
                    updateTableLabelStatusFail(errorMsg, tableId, dataStatusDate, batchNo);
                    LOGGER.error(errorMsg);
                    throw new TagException(errorMsg);

                }

                LOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4.3.2 :插入临时表 {}>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>",
                        t1DstTable);
                baseDao.dbDisableLog(t1DstTable);
                lableDao.insertIntoTempTable(" insert into " + t1DstTable + " " + insertSql.toString());
                LOGGER.info(
                        ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4.2 : 本批次标签宽表和上一次标签宽表合并，生成到临时表2>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

                LOGGER.info(
                        ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4.2.1 : 获取上一批次标签字段和类型>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                String expCols = " OP_TIME, " + StringConstant.JOIN_ID_NAME + ",CITY_ID," + StringConstant.PHONE_NO
                        + "," + StringConstant.DIM_COL_INSERT;

                StringBuilder expColsSql = new StringBuilder("1=1 ");

                expCols = expCols + Joiner.on("").join(insertColArr.get(tableId));
                expCols = expCols.replaceAll(" ", "");
                for (String str : expCols.split(",")) {
                    expColsSql.append(" and COLNAME<>").append("'" + str.toUpperCase() + "'");
                }
                StringBuilder oldDstDdl = new StringBuilder();
                StringBuilder oldDstInsert = new StringBuilder();
                StringBuilder oldDstSelect = new StringBuilder();

                List<Map<String, Object>> labelSqlResults = lableDao.getColsByDictTab(StringConstant.SCHEMA_DW,
                        dstTableName.toString(), expColsSql.toString());

                if (labelSqlResults.size() <= 0) {
                    break;
                }

                for (Map<String, Object> row : labelSqlResults) {

                    String colName = String.valueOf(row.get("COLNAME"));
                    String colDataTypeName = String.valueOf(row.get("TYPENAMW"));
                    String colDataLength = String.valueOf(row.get("TYPENAME_ONE"));
                    String colDataPrecision = String.valueOf(row.get("TYPENAME_TWO"));

                    String colType = com.ai.tag.utils.TagStringUtils.getDataTypeByParam(colDataTypeName, colDataLength,
                            colDataPrecision);

                    oldDstDdl.append(" , ").append(colName).append(" ").append(colType).append(" ");
                    oldDstInsert.append(" , ").append(colName).append(" ");
                    oldDstSelect.append(" , T1.").append(colName).append(" ");
                }

                LOGGER.info(
                        ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4.2.2 : 两个批次宽表做join合并>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                columnStr.append(oldDstDdl);

                commonService.createTempTable(t2DstTable, columnStr.toString(), StringConstant.TBS_DW,
                        StringConstant.TBS_INDEX, "0", distKey.toString(), 1);

                StringBuilder sql = new StringBuilder();
                sql.append(" insert into ").append(t2DstTable).append(" (OP_TIME,").append(joinColInsert)
                        .append(",CITY_ID,").append(StringConstant.DIM_COL_INSERT)
                        .append(TagStringUtils.trimRightByChar(oldDstInsert.toString().trim(), ","))
                        .append(TagStringUtils.trimRightByChar(Joiner.on("").join(insertColArr.get(tableId)).trim(),
                                ","))
                        .append(" ) select date('").append(optime).append("'),").append(joinColSelect)
                        .append(",T1.CITY_ID,").append(StringConstant.DIM_COL_SELECT)
                        .append(TagStringUtils.trimRightByChar(oldDstSelect.toString().trim(), ","))
                        .append(TagStringUtils.trimRightByChar(Joiner.on("").join(joinColArr.get(tableId)).trim(),
                                ","))
                        .append(" from ").append(dstTable).append(" T1 left join ").append(t1DstTable)
                        .append(" T2 ").append(" on T1.").append(StringConstant.JOIN_ID_NAME).append(" = T2.")
                        .append(StringConstant.JOIN_ID_NAME);

                baseDao.dbDisableLog(dstTable.toString());
                lableDao.insertIntoTempTable(sql.toString());

            } else {

                LOGGER.info(
                        ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4.1 : 表不存在，第一次生成>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                if (dstExistsFlag == StringConstant.FLAG_TRUE) {
                    indexDao.updateDstStatus(tableId, StringConstant.TABLE_TYPE_LABEL_TABLE,
                            StatusConstant.LABEL_DST_SEC, StatusConstant.LABEL_DST_RUNNING, dataStatusDate, batchNo,
                            String.valueOf(StringConstant.FLAG_FALSE), Thread.currentThread().getId(), "");

                } else {
                    indexDao.insertDstStatus(tableId, StringConstant.TABLE_TYPE_LABEL_TABLE,
                            StatusConstant.LABEL_DST_RUNNING, dataStatusDate, batchNo, Thread.currentThread().getId());
                }

                LOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4.1.1 : 创建目标表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                commonService.createTempTable(t2DstTable, columnStr.toString(), StringConstant.TBS_DW,
                        StringConstant.TBS_INDEX, "0", distKey.toString(), 1);

                baseDao.dbDisableLog(t2DstTable);
                lableDao.insertIntoTempTable("insert into " + t2DstTable + " " + insertSql.toString());

            }

            LOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4.3 : rename目标表为临时表3-->T3_目标表，rename 临时表2为目标表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            if (commonService.isTableExists(dstTable.toString())) {
                baseDao.renameTable(tableSchemaArr.get(tableId), dstTableName.toString(), t3DstTableName);
            }

            baseDao.renameTable(tableSchemaArr.get(tableId), t2DstTableName, dstTableName.toString());

            LOGGER.info(
                    ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4.4 : drop临时表1，临时表3...T1={},T3={}>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>",
                    t1DstTable, t3DstTable);

            if (dstExistsFlag == StringConstant.FLAG_TRUE && commonService.isTableExists(dstTable.toString())) {
                commonService.truncateTable(t1DstTable);
                commonService.dropTable(t1DstTable);
            }

            if (commonService.isTableExists(t3DstTable)) {
                commonService.truncateTable(t3DstTable);
                commonService.dropTable(t3DstTable);
            }

            LOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 3.3.6 : 建立索引>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            baseDao.createIndex(dstTable + "_I", dstTable.toString(), distKey.toString());

            // #更新目标表统计信息
            baseDao.runstatsTable(dstTable.toString());

            LOGGER.info(
                    ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>3.4、更新非0/1签，非属性签标签状态为已生效，更新属性签中的虚拟签为已生效，按照标签表更新>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

            if (!StringUtils.isEmpty(updateStateLabelListArray.get(tableId))) {

                StringBuilder update_label_id_list = new StringBuilder(" 1=0 ");

                for (String str : updateStateLabelListArray.get(tableId)) {

                    update_label_id_list.append("  or label_id = ").append(str);
                }

                if (updateLabelDataStateId.get(tableId) != null
                        && updateLabelDataStateId.get(tableId) == StringConstant.FLAG_TRUE) {

                    lableDao.updateTagDataStatusOk(update_label_id_list.toString());
                }

                lableDao.editTagStatus(dataStatusDate, batchNo, update_label_id_list.toString());
            }
            LOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4.6 : 修改标签状态>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

            lableDao.updateTagStatus(dataStatusDate, batchNo,
                    TagStringUtils.trimByChar(Joiner.on("").join(labelIdListStrArr.get(tableId)).trim(), ","));

            LOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4.7 : 修改指标表状态表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            lableDao.recoverTagStatusTable(batchNo, usedIdxTableSql.toString(), dataStatusDate);

            LOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> 4.8 : 修改目标表状态表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

            indexDao.updateDstStatus(tableId, StringConstant.TABLE_TYPE_LABEL_TABLE, StatusConstant.LABEL_RULE_RUNNING,
                    StatusConstant.LABEL_DST_SEC, dataStatusDate, batchNo, String.valueOf(StringConstant.FLAG_TRUE),
                    Thread.currentThread().getId(), "");

        }

        LOGGER.info(
                ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> 5、若无待处理表，则将 CI_MDA_SYS_TABLE 表中状态归位，程序结束>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        lableDao.recoverTagStatusTableOk(batchNo, dataDateIso);

        LOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Step 7 : 标签状态归位>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        updateLabelStatusFail("标签状态未归位，待查", dataStatusDate, batchNo, "");

        LOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 8 : 标签表状态归位>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

        LOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 9 : 更新最新数据周期>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        if (tbCycle == StringConstant.CYCLE_DEF_FT_DAILY) {
            lableDao.updateDataCycleDate(opTime);
        } else if (tbCycle == StringConstant.CYCLE_DEF_FT_MONTHLY) {
            lableDao.updateDataCycleDate(dataDateIso);
        }
        LOGGER.info("程序完成 !! ======>coc_d_dw_label_table_rules_yyyymmdd 程序耗时{}分钟",
                (System.currentTimeMillis() - start) / 1000 / 60);
        return true;
    }

    /**
     * 功能描述: <br>
     *
     * @param errorMsg
     * @param tableId
     * @param dataStatusDate
     * @param batchNo
     */
    private void updateTableLabelStatusFail(String errorMsg, String tableId, String dataStatusDate, long batchNo) {
        lableDao.updateTagStatusFail(errorMsg, dataStatusDate, batchNo, tableId);
    }

    /**
     * 功能描述: <br>
     *
     * @param errorMsg
     * @param dataStatusDate
     * @param batchNo
     * @param labelListStr
     */
    private void updateLabelStatusFail(String errorMsg, String dataStatusDate, long batchNo, String labelListStr) {

        StringBuilder whereLabelSql = new StringBuilder(" 1=0 ");

        labelListStr = labelListStr.replaceAll(" ", "");
        if (!"".equals(labelListStr.trim())) {
            String[] whereLabelList = labelListStr.split(",");
            for (String str : whereLabelList) {
                whereLabelSql.append(" or LABEL_ID=").append(str);
            }
        } else {
            whereLabelSql.append(" or 1=1");
        }

        lableDao.updateTagsStatusWithErrorMsg(errorMsg, dataStatusDate, batchNo, whereLabelSql.toString());
    }

}
