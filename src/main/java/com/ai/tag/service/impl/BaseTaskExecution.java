/* Copyright(c) 2017 AsiaInfo Technologies (China), Inc.
 * All Rights Reserved.
 * FileName: 
 * Author:   
 * Date:    
 * Description: 
 */
package com.ai.tag.service.impl;

import java.util.Date;

import javax.annotation.Resource;

import com.ai.tag.common.Job;
import com.ai.tag.common.TagConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.ai.tag.common.TagException;
import com.ai.tag.dao.IBaseDao;
import com.ai.tag.service.IBaseTaskExecution;
import com.ai.tag.utils.DateFormatUtils;


/**
 * 
 * 调度任务总入口<br>
 * 主要完成:</br>
 * 1、获取入参 2、入参格式检查 3、公共参数初始化
 *
 * @author chensf
 */
public abstract class BaseTaskExecution implements IBaseTaskExecution {

    private static final Logger TAGLOGGER = LoggerFactory.getLogger(BaseTaskExecution.class);

    protected String dataStatusDate;
    protected String dataDateIso;
    protected long batchNo;

    // 脚本参数

    protected String opTime;// 账期
    protected String rCmdId;// 调度号
    protected String cmdId;// 任务号
    protected String tbCode;// 宽表代码，多个宽表用','分割
    protected int tbCycle;// 统计周期;1:日,2:月
    protected String reRunTbCode;// 需要重跑的宽表源表CODE,用','分隔
    protected String reRunIndexCode;// 需要重跑的指标CODE,用','分隔
    protected long pid; //当前线程号

    @Resource
    protected IBaseDao baseDao;
    
    protected void getInputParams(Job job) {
        this.opTime = StringUtils.isEmpty(job.getParam("opTime")) ? opTime : (String) job.getParam("opTime");
        this.rCmdId = StringUtils.isEmpty(job.getParam("rCmdId")) ? rCmdId : (String) job.getParam("rCmdId");
        this.cmdId = StringUtils.isEmpty(job.getParam("cmdId")) ? cmdId : (String) job.getParam("cmdId");
        this.tbCycle = StringUtils.isEmpty(job.getParam("tbCycle")) ? tbCycle
                : Integer.valueOf((String) job.getParam("tbCycle"));
        this.pid = Thread.currentThread().getId();
        
        // opTime rCmdId cmdId tbCycle 不能为空
        Assert.notNull(opTime, "账期参数opTime不能为空");
        Assert.notNull(rCmdId, "调度号rCmdId不能为空");
        Assert.notNull(cmdId, "任务号cmdId不能为空");
        Assert.notNull(tbCycle, "统计周期tbCycle不能为空");

        this.tbCode = StringUtils.isEmpty(job.getParam("tbCode")) ? tbCode : String.valueOf(job.getParam("tbCode"));
        this.reRunTbCode = StringUtils.isEmpty(job.getParam("reRunTbCode")) ? reRunTbCode
                : (String) job.getParam("reRunTbCode");
        this.reRunIndexCode = StringUtils.isEmpty(job.getParam("reRunIndexCode")) ? reRunIndexCode
                : (String) job.getParam("reRunIndexCode");

    }

    @Override
    public void init(String date, int cycleType) {
        TAGLOGGER.info(">>>>>>>>>>>>>> 初始化开始>>>>>>>>>>>>>>");
        try {
            dataStatusDate = DateFormatUtils.opTimeDateFormat(date, cycleType);
            if(cycleType == TagConstant.DAY_FLAG) {
                Date d = DateFormatUtils.strToDate_YYYYMMDD(dataStatusDate);
                dataDateIso = DateFormatUtils.dateToStr_YYYY_MM_DD(d);
            }
            if(cycleType == TagConstant.MONTH_FLAG){
                Date d = DateFormatUtils.strToDate_YYYYMMDD(dataStatusDate + "01");
                dataDateIso = DateFormatUtils.dateToStr_YYYY_MM_DD(d);
            }
            TAGLOGGER.info(">>>>处理时间:{}, 处理时间ISO：{}", this.dataStatusDate, this.dataDateIso);
            // 获取当前程序批次
            this.batchNo = baseDao.getNextSequence();
        } catch (Exception e) {
            throw new TagException(e.getMessage());
        }
        TAGLOGGER.info(">>>>当前批次:{}", batchNo);
        TAGLOGGER.info(">>>>>>>>>>>>>> 初始化结束>>>>>>>>>>>>>>");
    }
    
}
