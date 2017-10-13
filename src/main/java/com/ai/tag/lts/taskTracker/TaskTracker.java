///* Copyright(c) 2017 AsiaInfo Technologies (China), Inc.
// * All Rights Reserved.
// * FileName:
// * Author:
// * Date:     2017年2月23日 上午11:38:55
// * Description:
// */
//package com.ai.tag.lts.taskTracker;
//
//import com.ai.tag.common.TagException;
//import com.ai.tag.service.IBaseTaskExecution;
//import com.ai.tag.utils.SpringUtils;
//import com.github.ltsopensource.core.domain.Action;
//import com.github.ltsopensource.core.domain.Job;
//import com.github.ltsopensource.tasktracker.Result;
//import com.github.ltsopensource.tasktracker.logger.BizLogger;
//import com.github.ltsopensource.tasktracker.runner.JobContext;
//import com.github.ltsopensource.tasktracker.runner.JobRunner;
//import com.github.ltsopensource.tasktracker.runner.LtsLoggerFactory;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
///**
// * <br>
// *
// * @author
// */
//public class TaskTracker implements JobRunner{
//
//    private static final Logger LOGGER = LoggerFactory.getLogger(TaskTracker.class);
//
//    /*
//     * (non-Javadoc)
//     * @see
//     * com.github.ltsopensource.tasktracker.runner.JobRunner#run(com.github.ltsopensource.tasktracker.runner.JobContext)
//     */
//    @Override
//    public Result run(JobContext jobContext) throws Throwable {
//        BizLogger bizLogger = LtsLoggerFactory.getBizLogger();
//        Job job = jobContext.getJob();
//
//        try {
//            IBaseTaskExecution taskExecuter = null;
//            LOGGER.info("=====================执行coc_tools_rerun_update_state.tcl脚本开始===============");
//            taskExecuter = (IBaseTaskExecution) SpringUtils.getBean("resetDealStateBiz");
//            taskExecuter.executeTask(job);
//            LOGGER.info("=====================执行coc_tools_rerun_update_state.tcl脚本结束===============");
//
//            LOGGER.info("=====================执行coc_d_dw_index_table_rules_yyyymmdd.tcl脚本开始===============");
//            taskExecuter = (IBaseTaskExecution) SpringUtils.getBean("gatherBaseDatasBiz");
//            taskExecuter.executeTask(job);
//            LOGGER.info("=====================执行coc_d_dw_index_table_rules_yyyymmdd.tcl脚本结束===============");
//
//            LOGGER.info("=====================执行coc_d_dw_label_table_rules_yyyymmdd.tcl脚本开始===============");
//            taskExecuter = (IBaseTaskExecution) SpringUtils.getBean("dealLableService");
//            taskExecuter.executeTask(job);
//            LOGGER.info("=====================执行coc_d_dw_label_table_rules_yyyymmdd.tcl脚本结束===============");
//
//            LOGGER.info("=====================执行coc_d_label_user_count_yyyymmdd.tcl脚本开始===============");
//            taskExecuter = (IBaseTaskExecution) SpringUtils.getBean("gatherLabelUsersBiz");
//            taskExecuter.executeTask(job);
//            LOGGER.info("=====================执行coc_d_label_user_count_yyyymmdd.tcl脚本结束===============");
//
//            LOGGER.info("=====================执行coc_d_ci_label_brand_user_num.tcl脚本开始===============");
//            taskExecuter = (IBaseTaskExecution) SpringUtils.getBean("labelDataGenBiz");
//            taskExecuter.executeTask(job);
//            LOGGER.info("=====================执行coc_d_ci_label_brand_user_num.tcl脚本结束===============");
//
//
//
//        } catch (TagException e) {
//            bizLogger.info("执行失败");
//            LOGGER.error(">>>>>>>调度任务执行失败---->"+e.getMessage());
//            return new Result(Action.EXECUTE_EXCEPTION, e.getMessage());
//        }
//
//        return new Result(Action.EXECUTE_SUCCESS, "===============脚本按照顺序执行完成================");
//    }
//
//
//}
