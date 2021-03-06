package com.zxw.hbase2.filter;

import java.io.IOException;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.zxw.hbase2.PrepareData;

public class FilterListTest {
	//FilterList
	public static void main(String[] args) throws IOException {
		byte[] cf = Bytes.toBytes(PrepareData.CF_DEFAULT);
		byte[] column = Bytes.toBytes("key8");
		
		FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);//FilterList.Operator.MUST_PASS_ALL
		SingleColumnValueFilter filter1 = new SingleColumnValueFilter(
				cf, column, CompareOperator.EQUAL,Bytes.toBytes("value8"));
		SingleColumnValueFilter filter2 = new SingleColumnValueFilter(
				cf,column,CompareOperator.EQUAL,Bytes.toBytes("value9"));
		list.addFilter(filter1);
		list.addFilter(filter2);
		
		PrepareData.scanWithFilter(list);
	}

}
