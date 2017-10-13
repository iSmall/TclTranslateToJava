package com.ai.tag.service.impl;

import com.ai.tag.common.*;
import com.ai.tag.dao.ILabelDataGenerationDao;
import com.ai.tag.service.CommonService;
import com.ai.tag.utils.DateFormatUtils;
import com.ai.tag.utils.TagStringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

/**
 * 功能说明: coc 标签层 数据处理,对应coc_d_ci_label_brand_user_num.tcl<br>
 *
 * @author chensf
 */
@Service("labelDataGenBiz" )
public class LabelDataGenerationBizImpl extends BaseTaskExecution {
    private static final Logger TAGLOGGER = LoggerFactory.getLogger(LabelDataGenerationBizImpl.class);

    @Autowired
    protected ILabelDataGenerationDao labelDataGenerationDao;

    @Autowired
    private CommonService commonService;

    private String dateCycle = null;
    private String lastCycle = null;
    private String dwProduct = null;
    private String whereDwProduct = null;
    private String ciLabelStatTable = null;
    private String ciLabelStatLastCycleTab = null;
    private String ciLabelStaTempTable = null;
    private String updateNewstDateStr = null;
    private String oldestDateOfStat = null;
    private String ciLabelStatTemplateTable = null;

    @Override
    public void init(String date, int cycleType) {
        super.init(date, cycleType);
        TAGLOGGER.info("op_time" + " " + this.opTime);
        try {
            if (TagConstant.DAY_FLAG == cycleType) {
                TAGLOGGER.info("op_time" + " " + this.opTime);
                this.dateCycle = this.opTime;
                this.lastCycle = DateFormatUtils.getLastDay(this.opTime);
                this.dwProduct = TableNameConstant.DW_PRODUCT_DAY_TABLE_NAME + this.opTime;
                this.whereDwProduct = "";
                this.ciLabelStatTable = TableNameConstant.CI_LABEL_STAT_DM + "_" + this.opTime;
                this.ciLabelStatLastCycleTab = TableNameConstant.SCHEMA + ".CI_LABEL_STAT_DM_" + lastCycle;
                this.ciLabelStatTemplateTable = TableNameConstant.SCHEMA + ".CI_LABEL_STAT_DM_YYYYMMDD";
                this.updateNewstDateStr = "UPDATE  " + TableNameConstant.CI_NEWEST_LABEL_DATE
                        + " SET DAY_NEWEST_DATE ='" + this.opTime + "',DAY_NEWEST_STATUS=0 where DAY_NEWEST_DATE<='"
                        + this.opTime + "'";
                this.ciLabelStaTempTable = TableNameConstant.TEMP_CI_LABEL_STAT_DM + "_" + this.opTime;
                this.oldestDateOfStat = DateFormatUtils.getDateBySubtractDay(this.opTime, 31);
            } else if (TagConstant.MONTH_FLAG == cycleType) {
                TAGLOGGER.info("op_time" + " " + this.opTime);
                this.dateCycle = DateFormatUtils.dateToStr_YYYYMM(DateFormatUtils.strToDate_YYYYMMDD(this.opTime + "01" ));
                TAGLOGGER.info("dateCycle" + " " + this.dateCycle);
                this.lastCycle = DateFormatUtils.getLastMonth_YYYYMM(this.opTime + "01" );
                TAGLOGGER.info("lastCycle" + " " + this.lastCycle);
                this.dwProduct = TableNameConstant.DW_PRODUCT_MONTH_TABLE_NAME + dateCycle;
                this.whereDwProduct = "";
                this.ciLabelStatTable = TableNameConstant.CI_LABEL_STAT_MM + "_" + this.opTime;
                this.ciLabelStatLastCycleTab = TableNameConstant.SCHEMA + ".CI_LABEL_STAT_MM_" + lastCycle;
                this.ciLabelStatTemplateTable = TableNameConstant.SCHEMA + ".CI_LABEL_STAT_MM_YYYYMM";
                this.updateNewstDateStr = "UPDATE " + TableNameConstant.CI_NEWEST_LABEL_DATE
                        + " SET MONTH_NEWEST_DATE ='" + dateCycle
                        + "',MONTH_NEWEST_STATUS =0 where MONTH_NEWEST_DATE<='" + dateCycle + "'";
                this.ciLabelStaTempTable = TableNameConstant.TEMP_CI_LABEL_STAT_MM + "_" + this.opTime;
                this.oldestDateOfStat = DateFormatUtils.getDateBySubtractMonth_YYYYMM(this.opTime + "01", 3);
            } else {
                throw new TagException("cycleType 统计周期参数只能传入1或者2" );
            }
        } catch (Exception e) {
            throw new TagException(e.getMessage());
        }
        TAGLOGGER.info(">>>>当前批次:{}", batchNo);
        TAGLOGGER.info(">>>>>>>>>>>>>> 初始化结束>>>>>>>>>>>>>>" );
    }

    @Override
    public boolean executeTask(Job job) throws TagException {

        long start = System.currentTimeMillis();
        this.getInputParams(job);
        this.init(this.opTime, this.tbCycle);

        TAGLOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 3 : 取得标签列表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" );
        TAGLOGGER.info(
                ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 3.1.1 : 更新已在标签状态表中标签状态为RUNNING>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" );
        this.labelDataGenerationDao.updateLabelAsRunning(this.batchNo, this.pid, this.dateCycle, this.dataDateIso,
                this.tbCycle);

        TAGLOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 3.1.2 : 插入本批次可跑属性签子签>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" );
        this.labelDataGenerationDao.insertSubLabels(this.dateCycle, this.batchNo, this.pid, this.dataDateIso,
                this.tbCycle);

        TAGLOGGER.info(
                ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 3.2 : 获取本批次状态为RUNNING的标签列表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" );
        List<Map<String, Object>> runningLabelList = this.labelDataGenerationDao.getRunningLabelList(this.dateCycle,
                this.batchNo);
        if (runningLabelList.size() <= 0) {
            throw new TagException("本次处理标签列表为空，不做任何处理，退出" );
        }

        TAGLOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4 : 创建目标表，删除索引>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" );
        TAGLOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 4.1 : 插入状态到目标表状态表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" );
        try {
            this.labelDataGenerationDao.insertDstStatus("CI_LABEL_STAT", this.dateCycle,
                    StringConstant.TABLE_TYPE_LABEL_STAT_TABLE, StatusConstant.LABEL_STAT_RUNNING, this.batchNo,
                    this.pid);
        } catch (Exception e) {
            TAGLOGGER.error("修改目标表状态失败!,{}", e.getMessage());
        }


        if (!commonService.isTableExists(ciLabelStatLastCycleTab)) {
            String errorMsg = String.format("历史表{}不存在.请检查无历史表的原因!!!", ciLabelStatLastCycleTab);
            TAGLOGGER.error(errorMsg);
            throw new TagException(errorMsg);
        }

        // 创建目标表，删除索引
        this.createTableOrDelIndex();

        TAGLOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 5 创建临时表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" );
        this.createTmpTable(this.ciLabelStaTempTable, this.ciLabelStatTemplateTable);

        TAGLOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 6 : 统计本统计周期总用户数>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" );
        long userCount = this.labelDataGenerationDao.getUserCount(this.dwProduct);
        if (userCount <= 0) {
            TAGLOGGER.error("用户宽表 {} 中用户数为0，请检查  50120", this.dwProduct);
            return false;
        }
        TAGLOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 7 : 循环汇总标签数据>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" );
        List<Integer> labelIdList = new ArrayList<Integer>();
        this.processRunningLabelList(runningLabelList, labelIdList);

        TAGLOGGER.info(
                ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 8 : 关联上一周期数据，取得环比值并插入到目标表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" );
        this.processLastWeekDataRelation(labelIdList, userCount);

        TAGLOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 9 : 更新最新数据周期>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" );
        this.labelDataGenerationDao.updateDataCycle(this.updateNewstDateStr);

        TAGLOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 10 : 更新标签数据状态>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" );
        this.labelDataGenerationDao.updateLabelDataStatus(labelIdList);

        TAGLOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 11 : 更新标签状态>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" );
        this.labelDataGenerationDao.updateLabelStatus(this.dateCycle, this.batchNo, labelIdList);
        this.labelDataGenerationDao.deleteDstTableStatus(this.batchNo);

        TAGLOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 12 : 删除临时表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" );
        this.dropTempTable();

        TAGLOGGER.info("程序完成 !! ======>coc_d_ci_label_brand_user_num 程序耗时" +
                (System.currentTimeMillis() - start) / 1000 + "秒" );
        return true;
    }

    private void createTableOrDelIndex() {
        if (this.baseDao.doesTableExist(TagStringUtils.getScheamWithTabName(this.ciLabelStatTable),
                TagStringUtils.getTabNameWithSchema(this.ciLabelStatTable))) {
            if (!this.baseDao.isEmptyTable(this.ciLabelStatTable)) {
                this.createTmpTable(this.ciLabelStatTable, this.ciLabelStatTemplateTable);
                this.labelDataGenerationDao.insetDataIntoNewTempTable(this.ciLabelStatTable,
                        this.ciLabelStatLastCycleTab, this.oldestDateOfStat);
            } else {
                // 表存在，删除索引
                if (commonService.isIndexExists(this.ciLabelStatTable + "_I" )) {
                    this.baseDao.dropIndex(this.ciLabelStatTable + "_I" );
                }
            }

        } else {
            // 表不存在则新建表，插入历史数据
            this.createTmpTable(this.ciLabelStatTable, this.ciLabelStatTemplateTable);
            this.labelDataGenerationDao.insetDataIntoNewTempTable(this.ciLabelStatTable, this.ciLabelStatLastCycleTab,
                    this.oldestDateOfStat);
            //日-31 月-3
        }
    }

    private void createTmpTable(String tableName, String tempTable) {
        boolean doesTableExist = this.baseDao.doesTableExist(TagStringUtils.getScheamWithTabName(tableName),
                TagStringUtils.getTabNameWithSchema(tableName));
        if (doesTableExist) {
            this.baseDao.truncateTable(tableName);
            this.baseDao.dropTable(tableName);
        }

        String distKey = "CITY_ID,BRAND_ID,VIP_LEVEL_ID";
        this.baseDao.createTableFromTempTable(tableName, tempTable, StringConstant.TBS_TEMP, distKey,
                StringConstant.TBS_INDEX, null);
    }

    private void processRunningLabelList(List<Map<String, Object>> runningLabelList, List<Integer> labelIdList) {
        for (Map<String, Object> runningLabel : runningLabelList) {
            int labelId = Integer.valueOf(String.valueOf(runningLabel.get("LABEL_ID" )));
            String tableName = String.valueOf(runningLabel.get("TABLE_NAME" ));
            String columnName = String.valueOf(runningLabel.get("COLUMN_NAME" ));
            String attrVal = String.valueOf(runningLabel.get("ATTR_VAL" ));
            int labelTypeId = Integer.valueOf(String.valueOf(runningLabel.get("LABEL_TYPE_ID" )));
            String tableSchema = String.valueOf(runningLabel.get("TABLE_SCHEMA" ));

            if (!StringUtils.isEmpty(tableSchema) && String.valueOf(StringConstant.FLAG_NO_NULL).equals(tableSchema)) {
                tableSchema = TableNameConstant.SCHEMA;
            }
            labelIdList.add(labelId);
            // 用户数统计
            String sumStr = null;
            String fromStr = null;
            if (StringConstant.LABEL_TYPE_ONE_DIM == labelTypeId) {
                // ## 0/1型标签从count表中汇总
                sumStr = "SUM(" + columnName + ")";
                fromStr = "FROM " + tableSchema + "." + tableName + "_CNT_" + this.dateCycle + " t1";
            } else if (StringConstant.LABEL_TYPE_ATTR == labelTypeId) {
                // ## 多维型标签，在标签宽表中汇总
                sumStr = "COUNT(1)";
                fromStr = "FROM " + tableSchema + "." + tableName + "_" + this.dateCycle + " t1 WHERE char("
                        + columnName + ") = '" + attrVal + "'";
            }
            TAGLOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 7 循环内 : 插入目标表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" );
            TAGLOGGER.info(this.ciLabelStaTempTable + this.dateCycle + labelId + sumStr +
                    fromStr);
            this.labelDataGenerationDao.insertTargetTable(this.ciLabelStaTempTable, this.dateCycle, labelId, sumStr,
                    fromStr);
        }
    }

    private void processLastWeekDataRelation(List<Integer> labelIdList, long userCount) {
        TAGLOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 8.1 : 删除对应历史数据>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" );
        StringBuilder labeIdListStr = new StringBuilder("" );
        for (Integer labelId : labelIdList) {
            labeIdListStr.append("," + labelId);
        }
        this.labelDataGenerationDao.deleteHistoryData(this.ciLabelStatTable,
                TagStringUtils.trimByChar(labeIdListStr.toString(), "," ), dateCycle);
        TAGLOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 8.2 : 插入目标表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" );
        this.labelDataGenerationDao.insertTargetTable2(this.ciLabelStatTable, this.ciLabelStaTempTable,
                this.ciLabelStatLastCycleTab, this.lastCycle, userCount);
        TAGLOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 8.3 : 建立索引>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" );
        this.baseDao.createIndex(this.ciLabelStatTable + "_I", this.ciLabelStatTable, "data_date,label_id" );

        TAGLOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Step 8.4 : 更新统计信息>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" );
        this.baseDao.runstatsTable(this.ciLabelStatTable);
    }

    private void dropTempTable() {
        this.baseDao.truncateTable(this.ciLabelStaTempTable);
        this.baseDao.dropTable(this.ciLabelStaTempTable);
    }
}
