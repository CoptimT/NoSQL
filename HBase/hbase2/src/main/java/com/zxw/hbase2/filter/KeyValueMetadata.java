package com.zxw.hbase2.filter;

import java.io.IOException;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.DependentColumnFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.zxw.hbase2.PrepareData;

public class KeyValueMetadata {

	public static void main(String[] args) throws IOException {
		byte[] cf = Bytes.toBytes(PrepareData.CF_DEFAULT);
		byte[] column = Bytes.toBytes("key8");
		BinaryComparator familyBinaryComparator=new BinaryComparator(cf);
		BinaryComparator columnBinaryComparator=new BinaryComparator(column);
		BinaryComparator valueBinaryComparator=new BinaryComparator(Bytes.toBytes("value8"));
		
		//If an already known column family is looked for, use Get.addFamily(byte[]) directly rather than a filter.
		PrepareData.scanWithFilter(new FamilyFilter(CompareOperator.EQUAL, familyBinaryComparator));
		
		//查询存在且复合条件的列
		QualifierFilter qualifierFilter = new QualifierFilter(CompareOperator.EQUAL, columnBinaryComparator);
		PrepareData.scanWithFilter(qualifierFilter);
		
		//查询存在且复合条件的列
		ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter(Bytes.toBytes("key1"));
		PrepareData.scanWithFilter(columnPrefixFilter);
		
		byte[][] prefixes = new byte[][] {Bytes.toBytes("key1"), Bytes.toBytes("key9")};
		MultipleColumnPrefixFilter multipleColumnPrefixFilter=new MultipleColumnPrefixFilter(prefixes);
		PrepareData.scanWithFilter(multipleColumnPrefixFilter);
		
		ColumnRangeFilter columnRangeFilter=new ColumnRangeFilter(Bytes.toBytes("key5"), true, Bytes.toBytes("key7"), false);
		PrepareData.scanWithFilter(columnRangeFilter);
		
		//ColumnCountGetFilter 
        //a,无法在scan中使用，只能在Get中
        //b,若设为0，则无法返回数据，设为几就按服务器中存储位置取回前几列
		Get get = new Get(Bytes.toBytes("row10")); 
        get.setFilter(new ColumnCountGetFilter(5));  
		PrepareData.get(get);
		
		//ColumnPaginationFilter 
        //a,limit 表示返回列数 
        //b,offset 表示返回列的偏移量，如果为0，则全部取出，如果为1，则返回第二列及以后 
		PrepareData.scanWithFilter(new ColumnPaginationFilter(2,1));
		
		/** 
	    DependentColumnFilter（该过滤器有两个参数：Family和Qualifier,尝试找到该列所在的每一行， 
	        并返回该行具有相同时间戳的全部键值对。如果某一行不包含指定的列，则该行的任何键值对都不返回， 
	    dropDependentColumn-如果为true,Family和Qualifier的列不返回； 
	        该过滤器还可以有两个可选的参数-一个比较操作符和一个值比较器，用于Family和Qualifier 
	        的进一步检查，如果从属的列找到，其值还必须通过值检查，然后就是时间戳必须考虑） 
	    */
		PrepareData.scanWithFilter(new DependentColumnFilter(cf, column));
		PrepareData.scanWithFilter(new DependentColumnFilter(cf, column, true));
		PrepareData.scanWithFilter(new DependentColumnFilter(cf, column,false,CompareOperator.EQUAL,valueBinaryComparator));
	}
	
}
