package com.ai.tag.common;

/**
 * Tag 表名常量<br>
 *
 * @author chensf
 */
public class TableNameConstant {
	
	public static final String SCHEMA = "SCCOC";
	public static final String INDCATER = ".";
	// 状态表
	public static final String SRC_BATCH_MAPPING = "sccoc.DIM_COC_SRC_BATCH_MAPPING";
	public static final String INDEX_SOURCE_STATUS = "sccoc.DIM_COC_DATA_SOURCE_STATUS";// 指标源表状态表
	public static final String DST_TABLE_STATUS = "sccoc.DIM_TARGET_TABLE_STATUS";

	// 指标层
	public static final String DIM_COC_INDEX_MODEL_TABLE_CONF = "sccoc.DIM_COC_INDEX_MODEL_TABLE_CONF";
	public static final String DIM_COC_INDEX_INFO = "sccoc.DIM_COC_INDEX_INFO";
	public static final String DIM_COC_INDEX_TABLE_INFO = "sccoc.DIM_COC_INDEX_TABLE_INFO";
	public static final String COC_INDEX_STATUS = "sccoc.DIM_COC_INDEX_STATUS";// 标签层
	public static final String DIM_COC_LABEL_COUNT_RULES = "sccoc.DIM_COC_LABEL_COUNT_RULES";
	public static final String CI_MDA_SYS_TABLE = "sccoc.CI_MDA_SYS_TABLE";
	public static final String CI_MDA_SYS_TABLE_COLUMN = "sccoc.CI_MDA_SYS_TABLE_COLUMN";
	public static final String CI_LABEL_INFO = "sccoc.CI_LABEL_INFO";
	public static final String CI_LABEL_EXT_INFO = "sccoc.CI_LABEL_EXT_INFO";
	public static final String CI_APPROVE_STATUS = "sccoc.CI_APPROVE_STATUS";
	public static final String CI_NEWEST_LABEL_DATE = "sccoc.CI_NEWEST_LABEL_DATE";
	public static final String COC_LABEL_STATUS = "sccoc.DIM_COC_LABEL_STATUS";
	public static final String CI_LABEL_VERTICAL_COLUMN_REL = "sccoc.CI_LABEL_VERTICAL_COLUMN_REL";
	public static final String COC_LABEL_RERUN = "sccoc.CI_LABEL_STATUS_HISTORY";
	public static final String CI_LABEL_STAT_MM = "sccoc.CI_LABEL_STAT_MM";
	public static final String CI_LABEL_STAT_DM = "sccoc.CI_LABEL_STAT_DM";

	// 临时表
	public static final String TEMP_CI_LABEL_STAT_MM = "sccoc.TEMP_CI_LABEL_STAT_MM";
	public static final String TEMP_CI_LABEL_STAT_DM = "sccoc.TEMP_CI_LABEL_STAT_DM";

	// 序列数组
	public static final String DATA_BATCH = "sccoc.S_label_index_batch";
	
	/**
	 * 用户表月表 不含数据日期
	 */
	public static final String DW_PRODUCT_MONTH_TABLE_NAME = "AIAPP.DW_COC_PRODUCT_MSG_";
	
	/**
	 * 用户表日表 不含数据日期
	 */
	public static final String DW_PRODUCT_DAY_TABLE_NAME = "AIAPP.DW_COC_PRODUCT_MSG_";
	
}
