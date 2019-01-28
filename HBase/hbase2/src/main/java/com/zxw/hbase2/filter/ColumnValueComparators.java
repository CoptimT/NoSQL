package com.zxw.hbase2.filter;

import java.io.IOException;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import com.zxw.hbase2.PrepareData;

public class ColumnValueComparators {

	public static void main(String[] args) {
		try {
			RegexStringComparator regexStringComparator = new RegexStringComparator("value0.");// any value that starts with 'my'
			testColumnValueComparators("key2",regexStringComparator);
			
			SubstringComparator substringComparator = new SubstringComparator("value3");// looking for 'my value'
			testColumnValueComparators("key2",substringComparator);
			
			BinaryPrefixComparator binaryPrefixComparator = new BinaryPrefixComparator(Bytes.toBytes("value3"));
			testColumnValueComparators("key2",binaryPrefixComparator);
			
			BinaryComparator binaryComparator=new BinaryComparator(Bytes.toBytes("value3"));
			testColumnValueComparators("key2",binaryComparator);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void testColumnValueComparators(String columnQualifier,ByteArrayComparable comp) throws IOException {
		byte[] cf = Bytes.toBytes(PrepareData.CF_DEFAULT);
		byte[] column = Bytes.toBytes(columnQualifier);
		
		SingleColumnValueFilter filter = new SingleColumnValueFilter(cf,column,CompareOperator.EQUAL,comp);
		
		PrepareData.scanWithFilter(filter);
	}
}
