package com.ai.tag.utils;

import com.ai.tag.common.StringConstant;
import com.ai.tag.common.TagException;

import org.springframework.util.StringUtils;

public class TagStringUtils {

    /**
     * 获取带schema的表名 e.g SCCOC.DW_COC_INDEX_004_20161026
     * 
     * @param nameWithSchema
     * @return
     */
    public static String getTabNameWithSchema(String nameWithSchema) {
        int index = nameWithSchema.indexOf(".");
        return nameWithSchema.substring(index + 1).toUpperCase();
    }

    /**
     * 获取带schema的schema名 e.g SCCOC.DW_COC_INDEX_004_20161026
     * 
     * @param nameWithSchema
     * @return
     */
    public static String getScheamWithTabName(String nameWithSchema) {
        int index = nameWithSchema.indexOf(".");
        return nameWithSchema.substring(0, index).toUpperCase();
    }


    /**
     * 
     * 功能描述:根据传入参数拼接字段的数据类型 <br>
     * 〈功能详细描述〉
     *
     * @param dataTypeName 字段数据类型
     * @param dataLength 字段长度
     * @param dataPrecision 字段精度
     * @return
     */
    public static String getDataTypeByParam(String dataTypeName, String dataLength, String dataPrecision)
            throws TagException {

        dataLength = "".equals(dataLength) ? StringConstant.FLAG_NO_NULL + "" : dataLength;

        dataPrecision = "".equals(dataPrecision) ? StringConstant.FLAG_NO_NULL + "" : dataPrecision;

        if (!(StringConstant.FLAG_NO_NULL + "").equals(dataTypeName)) {
            if (!(StringConstant.FLAG_NO_NULL + "").equals(dataLength)
                    && !(StringConstant.FLAG_NO_NULL + "").equals(dataPrecision)) {
                return dataTypeName + "(" + dataLength + "," + dataPrecision + ")";
            } else if (!(StringConstant.FLAG_NO_NULL + "").equals(dataLength)
                    && (StringConstant.FLAG_NO_NULL + "").equals(dataPrecision)) {
                return dataTypeName + "(" + dataLength + ")";
            } else if ((StringConstant.FLAG_NO_NULL + "").equals(dataLength)
                    && !(StringConstant.FLAG_NO_NULL + "").equals(dataPrecision)) {
                throw new TagException("错误的数据类型，COLUMN_DATA_PRECISION非空,COLUMN_DATA_LENGTH为空");
            } else {
                return dataTypeName;
            }
        } else {
            throw new TagException("错误的数据类型，column_data_type_name为空");
        }

    }

    /**
     * 
     * 功能描述: 去掉字符串两端的特定字符<br>
     *
     * @param targetStr
     * @param separator
     * @return
     * @throws TagException
     */
    public static String trimByChar(String targetStr, String separator) throws TagException {

        if (StringUtils.isEmpty(targetStr)) {
            throw new TagException("method TagStringUtils.trimByChar input empty string");
        }

        targetStr = targetStr.startsWith(separator) ? targetStr.substring(1, targetStr.length()) : targetStr;

        targetStr = targetStr.endsWith(separator) ? targetStr.substring(0, targetStr.lastIndexOf(separator))
                : targetStr;

        return targetStr;

    }

    /**
     * 
     * 功能描述: 去掉字符串右边的特定字符<br>
     *
     * @param targetStr
     * @param separator
     * @return
     * @throws TagException
     */
    public static String trimRightByChar(String targetStr, String separator) throws TagException {
        if (StringUtils.isEmpty(targetStr)) {
            return targetStr;
//            throw new TagException("method TagStringUtils.trimByChar input empty string");
        }

        targetStr = targetStr.endsWith(separator) ? targetStr.substring(0, targetStr.lastIndexOf(separator))
                : targetStr;

        return targetStr;
    }

}
