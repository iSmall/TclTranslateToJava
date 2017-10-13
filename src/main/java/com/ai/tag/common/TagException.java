/* Copyright(c) 2017 AsiaInfo Technologies (China), Inc.
 * All Rights Reserved.
 * FileName: TagException.java
 * Author:   <a href="mailto:xiongjie3@asiainfo.com">xiongjie3</a>
 * Date:     2017年2月22日 下午4:02:05
 * Description: 自定义异常处理     
 */
package com.ai.tag.common;

/**
 * 自定义异常处理<br> 
 *
 * @author xiongjie3
 */
public class TagException extends RuntimeException{

    private static final long serialVersionUID = 1L;
    
    public TagException(){
        super();
    }
    
    public TagException(String msg){
        super(msg);
    }
    
    public TagException(Throwable throwable){
        super(throwable);
    }
    
    public TagException(String msg,Exception e){
        super(msg,e);
    }
    

}
