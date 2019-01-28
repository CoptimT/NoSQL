package com.zxw.hbase2.filter;

import java.io.IOException;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.zxw.hbase2.PrepareData;

public class KeyValueMetadata {

	public static void main(String[] args) throws IOException {
		BinaryComparator familyBinaryComparator=new BinaryComparator(Bytes.toBytes(PrepareData.CF_DEFAULT));
		BinaryComparator columnBinaryComparator=new BinaryComparator(Bytes.toBytes("key8"));
		
		//If an already known column family is looked for, use Get.addFamily(byte[]) directly rather than a filter.
		FamilyFilter familyFilter=new FamilyFilter(CompareOperator.EQUAL, familyBinaryComparator);
		testFilter(familyFilter);
		
		//查询存在且复合条件的列
		QualifierFilter qualifierFilter = new QualifierFilter(CompareOperator.EQUAL, columnBinaryComparator);
		testFilter(qualifierFilter);
		
		//查询存在且复合条件的列
		ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter(Bytes.toBytes("key1"));
		testFilter(columnPrefixFilter);
		
		byte[][] prefixes = new byte[][] {Bytes.toBytes("key1"), Bytes.toBytes("key9")};
		MultipleColumnPrefixFilter multipleColumnPrefixFilter=new MultipleColumnPrefixFilter(prefixes);
		testFilter(multipleColumnPrefixFilter);
		
		ColumnRangeFilter columnRangeFilter=new ColumnRangeFilter(Bytes.toBytes("key5"), true, Bytes.toBytes("key7"), false);
		testFilter(columnRangeFilter);
	}
	
	private static void testFilter(FilterBase filter) throws IOException {
		PrepareData.init();
		
		Scan scan = new Scan();
		scan.setFilter(filter);
		
		PrepareData.scan(scan);
		PrepareData.close();
	}
}
