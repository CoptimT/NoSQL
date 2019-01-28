package com.zxw.hbase2.filter;

import java.io.IOException;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.zxw.hbase2.PrepareData;

public class RowKeyFilter {

	public static void main(String[] args) throws IOException {
		PrepareData.init();
		
		BinaryComparator rowBinaryComparator=new BinaryComparator(Bytes.toBytes("row2"));
		RowFilter rowFilter=new RowFilter(CompareOperator.EQUAL, rowBinaryComparator);
		
		Scan scan = new Scan();
		scan.setFilter(rowFilter);
		
		PrepareData.scan(scan);
		PrepareData.close();
	}

}
