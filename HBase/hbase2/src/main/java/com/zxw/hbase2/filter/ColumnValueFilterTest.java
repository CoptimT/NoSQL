package com.zxw.hbase2.filter;

import java.io.IOException;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnValueFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.zxw.hbase2.PrepareData;

public class ColumnValueFilterTest {
	
	public static void main(String[] args) throws IOException {
		testSingleColumnValueFilter();
		//testColumnValueFilter();
	}
	
	/**
	 * SingleColumnValueFilter
	 * @throws IOException
	 */
	public static void testSingleColumnValueFilter() throws IOException {
		PrepareData.init();
		
		byte[] cf = Bytes.toBytes(PrepareData.CF_DEFAULT);
		byte[] column = Bytes.toBytes("key2");
		//不包含该字段按满足条件处理
		SingleColumnValueFilter filter1 = new SingleColumnValueFilter(
				cf, column, CompareOperator.EQUAL,Bytes.toBytes("value1"));
		Scan scan = new Scan();
		scan.setFilter(filter1);
		
		PrepareData.scan(scan);
		PrepareData.close();
		/**
			row0 -> t
			key0 -> <1548232717622,value0>
			----------
			row1 -> t
			          key0 -> <1548232717637,value0>
			          key1 -> <1548232717637,value1>
			----------
		 */
	}
	
	/**
	 * ColumnValueFilter
	 * @throws IOException
	 */
	public static void testColumnValueFilter() throws IOException {
		PrepareData.init();
		
		byte[] cf = Bytes.toBytes(PrepareData.CF_DEFAULT);
		byte[] column = Bytes.toBytes("key8");
		//不包含该字段按满足条件处理
		ColumnValueFilter filter1 = new ColumnValueFilter(
				cf, column, CompareOperator.EQUAL,Bytes.toBytes("value8"));
		Scan scan = new Scan();
		scan.setFilter(filter1);
		
		PrepareData.scan(scan);
		PrepareData.close();
		/**
			row8 -> t
			          key8 -> <1548232717778,value8>
			----------
			row9 -> t
			          key8 -> <1548232717793,value8>
			----------
		 */
	}
}
