/* Copyright(c) 2017 AsiaInfo Technologies (China), Inc.
 * All Rights Reserved.
 * FileName: ApplicationBoot.java
 * Author:   <a href="mailto:xiongjie3@asiainfo.com">xiongjie3</a>
 * Date:     2017年2月21日 上午11:13:39
 * Description: application boot class
 */
package com.ai.tag;

import com.ai.tag.common.Job;
import com.ai.tag.common.TagException;
import com.ai.tag.service.IBaseTaskExecution;
import com.ai.tag.utils.SpringUtils;
import com.github.ltsopensource.core.domain.Action;
import com.github.ltsopensource.tasktracker.Result;
import jdk.nashorn.internal.runtime.regexp.joni.ScanEnvironment;
import jdk.nashorn.internal.scripts.JO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.task.TaskExecutor;

import java.util.Random;
import java.util.Scanner;


/**
 * 程序启动类<br>
 * 程序主入口,加载spring配置文件等
 *
 * @author xiongjie3
 */
public class ApplicationBoot {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationBoot.class);


    public static void main(String[] args) {

        AbstractApplicationContext ac = new ClassPathXmlApplicationContext("config/spring.xml");
        ac.registerShutdownHook();
        Job job = new Job();
        Random random = new Random();
        Scanner scanner = new Scanner(System.in);

//        System.out.println("》》》》》》请输入参数，按‘账期’ + ' ' + ‘周期’的格式：》》》》》》");
        job.setJob("opTime", args[0]);
        job.setJob("tbCycle", args[1]);

        job.setJob("rCmdId", String.valueOf(random.nextInt() * 9000 + 1000));
        job.setJob("cmdId", Integer.toString(random.nextInt() * 9000 + 1000));

//        job.setJob("tbCode", args[4]);
//        job.setJob("reRunTbCode", args[5]);
//        job.setJob("reRunIndexCode", args[6]);
//        job.setJob("tclname", args[7]);


        try {
            IBaseTaskExecution taskExecuter = null;
            LOGGER.info("=====================执行coc_tools_rerun_update_state.tcl脚本开始===============");
            taskExecuter = (IBaseTaskExecution) SpringUtils.getBean("resetDealStateBiz");
            taskExecuter.executeTask(job);
            LOGGER.info("=====================执行coc_tools_rerun_update_state.tcl脚本结束===============");

            LOGGER.info("=====================执行coc_d_dw_index_table_rules_yyyymmdd.tcl脚本开始===============");
            taskExecuter = (IBaseTaskExecution) SpringUtils.getBean("gatherBaseDatasBiz");
            taskExecuter.executeTask(job);
            LOGGER.info("=====================执行coc_d_dw_index_table_rules_yyyymmdd.tcl脚本结束===============");

            LOGGER.info("=====================执行coc_d_dw_label_table_rules_yyyymmdd.tcl脚本开始===============");
            taskExecuter = (IBaseTaskExecution) SpringUtils.getBean("dealLableService");
            taskExecuter.executeTask(job);
            LOGGER.info("=====================执行coc_d_dw_label_table_rules_yyyymmdd.tcl脚本结束===============");

            LOGGER.info("=====================执行coc_d_label_user_count_yyyymmdd.tcl脚本开始===============");
            taskExecuter = (IBaseTaskExecution) SpringUtils.getBean("gatherLabelUsersBiz");
            taskExecuter.executeTask(job);
            LOGGER.info("=====================执行coc_d_label_user_count_yyyymmdd.tcl脚本结束===============");

            LOGGER.info("=====================执行coc_d_ci_label_brand_user_num.tcl脚本开始===============");
            taskExecuter = (IBaseTaskExecution) SpringUtils.getBean("labelDataGenBiz");
            taskExecuter.executeTask(job);
            LOGGER.info("=====================执行coc_d_ci_label_brand_user_num.tcl脚本结束===============");


        } catch (TagException e) {
            e.printStackTrace();
            LOGGER.error(">>>>>>>调度任务执行失败---->" + e.getMessage());
        }

    }

}
