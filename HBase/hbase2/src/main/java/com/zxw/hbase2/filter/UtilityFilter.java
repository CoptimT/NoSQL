package com.zxw.hbase2.filter;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

import com.zxw.hbase2.PrepareData;

public class UtilityFilter {

	public static void main(String[] args) throws IOException {
		PrepareData.init();
		
		//每行仅返回第一个列
		FirstKeyOnlyFilter firstKeyOnlyFilter=new FirstKeyOnlyFilter();
		
		Scan scan = new Scan();
		scan.setFilter(firstKeyOnlyFilter);
		
		PrepareData.scan(scan);
		PrepareData.close();
	}

}
