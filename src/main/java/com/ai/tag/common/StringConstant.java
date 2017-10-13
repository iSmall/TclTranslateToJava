package com.ai.tag.common;

public class StringConstant {

    public static final String DB_USER = "aiapp";

    /**
     * 字符串类型，注意引用时增加长度
     */
    public static final String DATA_TYPE_VARCHAR = "varchar";
    /**
     * 整型类型
     */
    public static final String DATA_TYPE_INT = "integer";
    /**
     * 小整型类型
     */
    public static final String DATA_TYPE_SMALLINT = "smallint";
    /**
     * 数值类型，注意引用时增加长度
     */
    public static final String DATA_TYPE_NUMBER = "decimal";
    /**
     * 长整型类型
     */
    public static final String DATA_TYPE_BIGINT = "bigint";
    public static final String DUAL_TABLE = "SYSIBM.SYSDUMMY1";

    // 周期常量定义
    /**
     * 未知周期
     */
    public static final int CYCLE_DEF_UNKNOWN = 0;
    /**
     * 日周期
     */
    public static final int CYCLE_DEF_FT_DAILY = 1;
    /**
     * 月周期
     */
    public static final int CYCLE_DEF_FT_MONTHLY = 2;
    /**
     * 日周期
     */
    public static final int CYCLE_DEF_DW_DAILY = 1;
    /**
     * 月周期
     */
    public static final int CYCLE_DEF_DW_MONTHLY = 2;

    // 标签类型
    /**
     * 0/1标签，通过规则算出标签是0还是1
     */
    public static final int LABEL_TYPE_ONE_DIM = 1;
    /**
     * 属性标签，通过规则取指标，不计算，前台会把维度展开到标签信息表，后台生成数据到标签宽表
     */
    public static final int LABEL_TYPE_ATTR = 3;
    /**
     * 纵表标签，数据由现场生成
     */
    public static final int LABEL_TYPE_VERT = 8;

    // 标签表类型
    /**
     * 用户签
     */
    public static final int TABLE_TYPE_USER_LABEL = 1;
    /**
     * 产品签，目前只有北京移动用，产品签配置在后台维护，采用前后台同步方式
     */
    public static final int TABLE_TYPE_PRODUCT_LABEL = 2;
    /**
     * 纵表标签表，目前只有浙江移动再用
     */
    public static final int TABLE_TYPE_VERT_LABEL = 8;
    /**
     * 虚拟TABLE，不是表
     */
    public static final int TABLE_TYPE_NO_TABLE = 0;
    /**
     * 指标表
     */
    public static final int TABLE_TYPE_INDEX_TABLE = 3;
    /**
     * 标签宽表
     */
    public static final int TABLE_TYPE_LABEL_TABLE = 4;
    /**
     * 标签统计宽表
     */
    public static final int TABLE_TYPE_LABEL_CNT_TABLE = 5;
    /**
     * 标签统计信息表
     */
    public static final int TABLE_TYPE_LABEL_STAT_TABLE = 6;
    /**
     * 已发布
     */
    public static final String APPROVE_STATUS_PUBLISH = "107";

    // 标签数据状态
    /**
     * 未生效
     * 
     */
    public static final String DATA_STATUS_NOT_EFFECT = "1";
    /**
     * 已生效
     */
    public static final String DATA_STATUS_EFFECT = "2";

    // 特殊规则
    /**
     * 父节点的规则由其子节点的规则合并(取或)而来
     */
    public static final String RULE_CODE_PARENT = "R_00000";
    /**
     * 无效标签不进行计算
     */
    public static final String RULE_CODE_NONE = "D_00000";

    /**
     * 程序中TRUE FALSE表示
     */
    public static final int FLAG_TRUE = 1;
    public static final int FLAG_FALSE = 0;

    // 指标源表/目标表表名后是否有'_',如果为1，无论表配置是否有后缀'_'，程序都会自动补全，原来表配置了'_'的，不受这个配置影响
    /**
     * 指标源表
     */
    public static final int FLAG_NEED_IDX_SRC_SUFFIX = 1;
    /**
     * 指标目标宽表
     */
    public static final int FLAG_NEED_IDX_DST_SUFFIX = 1;
    /**
     * 汇总时表示全部维度汇总的值
     */
    public static final int FLAG_ALL_NULL = -1;
    
    public static final int FLAG_NO_NULL = -9;

    public static final String LABEL_EFFEC_RULE_SQL = "and CURR_APPROVE_STATUS_ID = '107' and DATA_STATUS_ID in (1,2)";

    /**
     * 完成标签计算后是否更新CI_LABEL_INFO中的数据状态
     */
    public static final int FLAG_UPDATE_STATUS_IN_LABEL_INFO = 1;

    /**
     * 需要做前后台表编码映射，用于浙江更新纵表标签处理表编码不一致问题
     */
    public static final int FLAG_NEED_TABLE_ID_MAPPING = 1;

    // 用户标识字段\地市ID字段
    /**
     * 用户标识字段，用于表间关联(用户表中的用户标识字段名称，接口表的在DIM_COC_INDEX_TABLE_INFO中配置)
     */
    public static final String JOIN_ID_NAME = "PHONE_NO";
    /**
     * 用户标识字段类型，建议与源表中用户标识字段类型一致
     */
    public static final String JOIN_ID_TYPE = "varchar(20)";
    /**
     * 地市ID字段
     * 
     */
    public static final String CITY_ID_NAME = "CITY_ID";
    /**
     * 地市ID字段类型
     */
    public static final String CITY_ID_TYPE = "SMALLINT";
    /**
     * 源表中电话号码字段，可能与join_id_name一样
     */
    public static final String PHONE_NO = "PHONE_NO";
    /**
     * 电话号码字段类型，可能和JOIN_ID_TYPE一样
     */
    public static final String PHONE_NO_TYPE = "varchar(20)";

    public static final String DW_PRODUCT_MONTH_TABLE_NAME = "AIAPP.DW_COC_PRODUCT_MSG_"; // ## 用户表月表 不含数据日期
    public static final String DW_PRODUCT_DAY_TABLE_NAME = "AIAPP.DW_COC_PRODUCT_MSG_"; // ## 用户表日表 不含数据日期

    /**
     * 其他维度字段，含类型，create中使用
     */
    public static final String DIM_COL_CREATE_DDL = "BRAND_ID smallint,VIP_LEVEL_ID smallint";

    /**
     * 其他维度字段，insert中使用
     */
    public static final String DIM_COL_INSERT = "BRAND_ID,VIP_LEVEL_ID";

    /**
     * 其他维度字段，从指标源表select中使用
     */
    public static final String DIM_COL_SRC_TAB_SELECT = "t1.BRAND_ID,t1.VIP_SCALE_ID";
    /**
     * 其他维度字段，COC生成的宽表select中使用
     */
    public static final String DIM_COL_SELECT = "t1.BRAND_ID,t1.VIP_LEVEL_ID";
    /**
     * 按维度组合统计用户数用的group by 子句
     */
    public static final String DIM_COL_GROUP_SET = "cube(t1.city_id,$DIM_COL_SELECT)),999";
    /**
     * 其他维度字段，having子名中使用
     */
    public static final String DIM_COL_HAVING = "t1.brand_id is null or t1.vip_level_id is null";
    /**
     * 其他维度字段，select中使用，包含字段非空处理
     */
    public static final String DIM_COL_NVL_SELECT = "coalesce(t1.city_id,-1) city_id,coalesce(t1.brand_id,-1) brand_id,coalesce(t1.vip_level_id,-1) vip_level_id";
    
    /**
     * SCHEMA_DW
     */
    public static final String SCHEMA_DW = "SCCOC";
    
    /**
     * 临时表的表空间
     */
    public static final String TBS_TEMP  = "tbs_coc";
//    public static final String TBS_TEMP  = "USERSPACE1";

    /**
     * 索引表空间
     */
  public static final String TBS_INDEX  = "tbs_coc";
    
  //  public static final String TBS_INDEX  = "USERSPACE1";
    
    public static final String TBS_DW  = "tbs_coc";
    
//    public static final String TBS_DW  = "USERSPACE1";
    
    /**
     * 针对dw、dwd层表，如果有查询应用，则需要建index，但有些地方可能不需要，所以定义这个变量来控制
     */
    public static final int FLAG_NEED_CREATE_INDEX  = 1;
    
    
    /**
     * 如果需要根据配置的批次和数据源映射表跑数据，设为1，否则设为0
     */
    public static final int FLAG_RUN_WITH_BATCH_MAPPING = 1;

}
