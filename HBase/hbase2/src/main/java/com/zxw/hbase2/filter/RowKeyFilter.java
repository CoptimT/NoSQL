package com.zxw.hbase2.filter;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RandomRowFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.zxw.hbase2.PrepareData;

public class RowKeyFilter {

	public static void main(String[] args) throws IOException {
		BinaryComparator rowBinaryComparator=new BinaryComparator(Bytes.toBytes("row5"));
		RowFilter rowFilter=new RowFilter(CompareOperator.EQUAL, rowBinaryComparator);
		PrepareData.scanWithFilter(rowFilter);
		
		//RowKey前缀
		PrepareData.scanWithFilter(new PrefixFilter(Bytes.toBytes("row1")));
		
		/*FuzzyRowFilter 模糊row查询,Pair中第一个参数为模糊查询的string, 第二个参数byte[]中
		    装与string位数相同的数值0或1,0表示该位必须与string中值相同，1表示可以不同*/
		List<Pair<byte[], byte[]>> pairs=Arrays.asList(new Pair<byte[], byte[]>(
				Bytes.toBytes("new1"),new byte[]{1, 1, 1, 0}));
		PrepareData.scanWithFilter(new FuzzyRowFilter(pairs));
		
		//指定stopRow在scan时从头扫描全部返回，直到stopRow停止（stopRow这行也会返回，然后scan停止）
		PrepareData.scanWithFilter(new InclusiveStopFilter(Bytes.toBytes("row3")));
		
		//rowkey几率
		PrepareData.scanWithFilter(new RandomRowFilter(0.2f));
	}

}
