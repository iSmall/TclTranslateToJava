///* Copyright(c) 2017 AsiaInfo Technologies (China), Inc.
// * All Rights Reserved.
// * FileName: TaskTrackerTest.java
// * Author:   <a href="mailto:xiongjie3@asiainfo.com">xiongjie3</a>
// * Date:     2017年2月27日 下午2:45:24
// * Description: //模块目的、功能描述
// * History: //修改记录
// * <author>      <time>      <version>    <desc>
// * 修改人姓名             修改时间            版本号                  描述
// */
//package com.ai.tag.lts.jobClient;
//
//import com.alibaba.fastjson.JSON;
//import com.github.ltsopensource.core.domain.Job;
//import com.github.ltsopensource.tasktracker.Result;
//import com.github.ltsopensource.tasktracker.runner.JobContext;
//import com.github.ltsopensource.tasktracker.runner.JobRunner;
//import com.github.ltsopensource.tasktracker.runner.JobRunnerTester;
//
//import java.util.UUID;
//
//import org.springframework.context.support.AbstractApplicationContext;
//import org.springframework.context.support.ClassPathXmlApplicationContext;
//
///**
// * 〈一句话功能简述〉<br>
// * 〈功能详细描述〉
// *
// * @author xiongjie3
// * @see [相关类/方法]（可选）
// * @since [产品/模块版本] （可选）
// */
//public class TaskTrackerTest extends JobRunnerTester{
//
//
//    public static void main(String[] args) throws Throwable {
//        //  Mock Job 数据
//        JobContext jobContext = new JobContext();
//        Job job = new Job();
//        String uuid = UUID.randomUUID().toString();
//
//        // 运行测试
//        TaskTrackerTest tester = new TaskTrackerTest();
//        job.setTaskId(uuid);
//        job.setParam("opTime", "20170304");
//        job.setParam("rCmdId", "1");
//        job.setParam("cmdId", "1");
//        job.setParam("tbCycle", "1");
//        jobContext.setJob(job);
//
//        Result result = tester.run(jobContext);
//        System.out.println(JSON.toJSONString(result));
//
//
//
//    }
//
//    @Override
//    protected void initContext() {
//        AbstractApplicationContext ac = new ClassPathXmlApplicationContext("classpath:config/spring.xml");
//    }
//
//    @Override
//    protected JobRunner newJobRunner() {
//        return new TaskTracker();
//    }
//
//}
