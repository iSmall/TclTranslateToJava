package com.ai.tag.common; // //

/**
 * Tag 状态值常量<br>
 *
 * @author chensf
 */
public class StatusConstant {

	public static final int IDX_SRC_READY = 1; // // ##指标源表准备好
	public static final int IDX_DST_READY = 2; // // ##指标目标表可以进行操作
	public static final int IDX_DST_SEC = 3; // // ##指标目标表生成完成，同一程序正在生成其他表
	public static final int IDX_DST_RUNNING = 11; // // ##指标宽表生成中
	public static final int IDX_DST_FAIL = 991; // // ##指标宽表失败
	public static final int IDX_DST_USING = 21; // // ##指标宽表使用中
	public static final int LABEL_DST_FAIL = 992; // // ##标签宽表生成失败
	public static final int LABEL_DST_SEC = 5; // // ##标签目标表生成完成，同一程序正在生成其他表
	public static final int LABEL_DST_RUNNING = 12; // // ##标签宽表生成中
	public static final int LABEL_DST_USING = 22; // // ##标签宽表使用中
	public static final int LABEL_CNT_DST_SEC = 7; // // ##标签统计目标表生成完成，同一程序正在生成其他表
	public static final int LABEL_CNT_DST_RUNNING = 13; // // ##标签统计宽表生成中
	public static final int LABEL_CNT_DST_FAIL = 993; // // ##标签统计宽表生成中
	public static final int LABEL_STAT_RUNNING = 14; // // ##标签统计信息表生成中

	public static final int INDEX_RUNNING = 11; // // ##指标字段正在生成
	public static final int INDEX_SEC = 3; // // ##指标字段生成成功
	public static final int INDEX_FAIL = 891; // // ##指标字段生成失败

	public static final int LABEL_WAIT_RERUN = 20; // // ##标签待重跑，此为重跑前状态，此时标签可用，等待重跑
	public static final int LABEL_RULE_FAIL = 892; // // ##标签宽表字段生成失败
	public static final int LABEL_RULE_RUNNING = 12; // // ##标签宽表字段生成中
	public static final int LABEL_RULE_SEC = 4; // // ##标签宽表字段生成成功

	public static final int LABEL_RERUN_NO_SEND_NOTICE = 0; // // ##标签重跑通知未发送，前台用
	public static final int LABEL_RERUN_SEC_SEND_NOTICE = 1; // // ##标签重跑通知已发送，前台用

	public static final int LABEL_CNT_RUNNING = 13; // // ##标签统计宽表字段生成中
	public static final int LABEL_CNT_SEC = 5; // // ##标签统计宽表字段生成成功
	public static final int LABEL_CNT_FAIL = 893; // // ##标签统计宽表字段生成失败

	public static final int LABEL_CUBE_RUNNING = 14; // // ##标签汇总统计中
	public static final int LABEL_CUBE_SEC = 6; // // ##标签汇总统计成功
	public static final int LABEL_CUBE_FAIL = 894; // // ##标签汇总统计失败

	public static final int VERT_LABEL_FAIL = 895; // // ##纵表标签状态更新失败
	public static final int VERT_LABEL_RUNNING = 15; // // ##纵表标签状态更新中

	public static final int LABEL_FT_SEC = 1; // // ##标签生成成功
	public static final int LABEL_FT_FAIL = 0; // // ##标签生成失败
	
	
	public static final int USER_LABEL=        1   ; // ## 用户签
	public static final int PRODUCT_LABEL=     2   ; // ## 产品签，目前只有北京移动用，产品签配置在后台维护，采用前后台同步方式
	public static final int VERT_LABEL=        8   ; // ## 纵表标签表，目前只有浙江移动再用
	public static final int NO_TABLE=          0   ; // ## 虚拟TABLE，不是表
	public static final int INDEX_TABLE=       3   ; // ## 指标表
	public static final int LABEL_TABLE=       4   ; // ## 标签宽表
	public static final int LABEL_CNT_TABLE=   5   ; // ## 标签统计宽表
	public static final int LABEL_STAT_TABLE=  6   ; // ## 标签统计信息表

}
