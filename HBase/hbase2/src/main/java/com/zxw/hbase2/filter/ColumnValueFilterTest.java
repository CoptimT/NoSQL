package com.zxw.hbase2.filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnValueFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SkipFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.TimestampsFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.zxw.hbase2.PrepareData;

public class ColumnValueFilterTest {
	
	public static void main(String[] args) throws IOException {
		testSingleColumnValueFilter();
		testColumnValueFilter();
		
		//ValueFilter 按value全数据库搜索（全部列的value均会被检索） 
		PrepareData.scanWithFilter(new ValueFilter(
				CompareOperator.EQUAL,new BinaryComparator(Bytes.toBytes("value8"))));
		
		//该过滤器需要配合其他过滤器来使用
		/** 
	    SkipFilter 根据整行中的每个列来做过滤，只要存在一列不满足条件，整行都被过滤掉。 
	        例如，如果一行中的所有列代表的是不同物品的重量，则真实场景下这些数值都必须大于零，我们希望将那些
	        包含任意列值为0的行都过滤掉。 在这个情况下，我们结合ValueFilter和SkipFilter共同实现该目的： 
	    scan.setFilter(new SkipFilter(new ValueFilter(CompareOperator.NOT_EQUAL,new BinaryComparator(Bytes.toBytes(0)))); 
	    */  
		SkipFilter skipFilter=new SkipFilter(new ValueFilter(
				CompareOperator.EQUAL,new SubstringComparator("value")));
		PrepareData.scanWithFilter(skipFilter);
		
		/*TimestampsFilter 
        a，按时间戳搜索数据库 
        b，需设定List<Long> 存放所有需要检索的时间戳*/
		List<Long> ls = new ArrayList<Long>();  
        ls.add((long)1548661917423L);  
        ls.add((long)1548661917434L);
        PrepareData.scanWithFilter(new TimestampsFilter(ls));
        
        /** WhileMatchFilter 相当于while执行，直到不match就break返回了。 */  
        PrepareData.scanWithFilter(new WhileMatchFilter(new ValueFilter(
        		CompareOperator.EQUAL,new SubstringComparator("value1"))));
	}
	
	/**
	 * SingleColumnValueFilter 返回全部列
	 * SingleColumnValueExcludeFilter
	 * 用来查找并返回指定条件的列的数据
        a，如果查找时没有该列，两种filter都会把该行所有数据返回 
        b，如果查找时有该列，但是不符合条件，则该行所有列都不返回 
        c，如果找到该列，并且符合条件，前者返回所有列，后者返回除该列以外的所有列 
	 */
	public static void testSingleColumnValueFilter() throws IOException {
		byte[] cf = Bytes.toBytes(PrepareData.CF_DEFAULT);
		byte[] column = Bytes.toBytes("key2");
		
		//不包含该字段按满足条件处理
		SingleColumnValueFilter filter = new SingleColumnValueFilter(
				cf, column, CompareOperator.EQUAL,Bytes.toBytes("value1"));
		PrepareData.scanWithFilter(filter);
		
		SingleColumnValueExcludeFilter filter1 = new SingleColumnValueExcludeFilter(
				cf, column, CompareOperator.EQUAL,new SubstringComparator("value2")); 
		PrepareData.scanWithFilter(filter1);
	}
	
	/**
	 * ColumnValueFilter 只返回查询列
	 */
	public static void testColumnValueFilter() throws IOException {
		byte[] cf = Bytes.toBytes(PrepareData.CF_DEFAULT);
		byte[] column = Bytes.toBytes("key8");
		//不包含该字段按满足条件处理
		ColumnValueFilter filter = new ColumnValueFilter(
				cf, column, CompareOperator.EQUAL,Bytes.toBytes("value8"));
		
		PrepareData.scanWithFilter(filter);
	}
}
