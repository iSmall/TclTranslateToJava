package com.ai.tag.dao;

import static org.junit.Assert.*;

import java.text.ParseException;

import org.junit.Test;

import com.ai.tag.utils.DateFormatUtils;

public class StrTest {

	@Test
	public void test() {
		fail("Not yet implemented");
	}
	
	@Test
	public void testStr() throws ParseException{
		String t = "sccoc.abb_fcccc_303030";
		
		System.out.println("================" +DateFormatUtils.getDateBySubtractDay("20170301", 31));
		
		System.out.println("================" +DateFormatUtils.getLastMonth_YYYYMM("20170301"));
		
		System.out.println("================" +DateFormatUtils.getDateBySubtractMonth_YYYYMM("20170301", 3));
	}
}
