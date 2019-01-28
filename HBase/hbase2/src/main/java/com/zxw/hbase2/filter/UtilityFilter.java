package com.zxw.hbase2.filter;

import java.io.IOException;

import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PageFilter;

import com.zxw.hbase2.PrepareData;

public class UtilityFilter {

	public static void main(String[] args) throws IOException {
		//每行仅返回第一个列
		PrepareData.scanWithFilter(new FirstKeyOnlyFilter());
		
		//KeyOnlyFilter 只取key值，不取value值，size正常 
	    //lenAsVal=true时，值为乱码；lenAsVal=false时，值为空字符串 
		PrepareData.scanWithFilter(new KeyOnlyFilter(false));
		
		//PageFilter 取回pageSize条数据
		PrepareData.scanWithFilter(new PageFilter(5));
	}

}
