package com.ai.tag.common;

import java.util.HashMap;

/**
 * Created by admin on 2017/6/29.
 */
public class Job {

    static private HashMap<String, Object> job = new HashMap<String, Object>();

    public static Object getParam(String key) {
        return job.get(key);
    }

    public static void setJob(String key, Object value) {
        job.put(key, value);
    }
}
