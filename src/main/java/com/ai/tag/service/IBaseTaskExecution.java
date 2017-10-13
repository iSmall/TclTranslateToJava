/* Copyright(c) 2017 AsiaInfo Technologies (China), Inc.
 * All Rights Reserved.
 * FileName: 
 * Author:   
 * Date:     
 * Description: 
 */
package com.ai.tag.service;

import com.ai.tag.common.Job;
import com.ai.tag.common.TagException;


/**
 * 
 *
 * @author chensf
 */
public interface IBaseTaskExecution {
    
	/**
	 * 初始化基础参数
	 */
	public void init(String date, int cycleType);
	
	/**
	 * 
	 * 功能描述: 调度任务执行入口<br>
	 * 调度任务执行主要逻辑实现
	 *
	 * @param job
	 * @return
	 * @throws TagException
	 * @see [相关类/方法](可选)
	 * @since [产品/模块版本](可选)
	 */
    public boolean executeTask(Job job)throws TagException;
}
