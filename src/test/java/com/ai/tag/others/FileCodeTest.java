/* Copyright(c) 2017 AsiaInfo Technologies (China), Inc.
 * All Rights Reserved.
 * FileName: FileCodeTest.java
 * Author:   <a href="mailto:xiongjie3@asiainfo.com">xiongjie3</a>
 * Date:     2017年3月14日 下午12:27:45
 * Description: //模块目的、功能描述      
 * History: //修改记录
 * <author>      <time>      <version>    <desc>
 * 修改人姓名             修改时间            版本号                  描述
 */
package com.ai.tag.others;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;

/**
 * 〈一句话功能简述〉<br> 
 * 〈功能详细描述〉
 *
 * @author xiongjie3
 * @see [相关类/方法]（可选）
 * @since [产品/模块版本] （可选）
 */
public class FileCodeTest {
    
    public static void main(String[] args) throws Exception {
        String path = "G:\\a_12500_201702_IOP-93002_00_001.dat";
        File file = new File(path);
        
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "GBK"));
        
        String line = null;
        while((line = reader.readLine()) != null){
            System.out.println(line);
        }
        
        
        
        
    }
    

}
