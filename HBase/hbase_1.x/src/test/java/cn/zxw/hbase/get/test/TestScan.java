package cn.zxw.hbase.get.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import cn.zxw.hbase.util.HBaseConnectUtil;

public class TestScan {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		try {
			Table table=HBaseConnectUtil.getTable("tsdb");
			//Filter filter = new ValueFilter(CompareOp.EQUAL,new SubstringComparator("103"));
			List<Filter> filters = new ArrayList<Filter>(2);
			Filter filter1 = new ValueFilter(CompareOp.GREATER,new BinaryComparator("103".getBytes()));
			Filter filter2 = new ValueFilter(CompareOp.LESS,new BinaryComparator("106".getBytes()));
			filters.add(filter1);
			filters.add(filter2);
			Scan scan = new Scan();
	        //scan.setFilter(new FilterList(filters));
	        ResultScanner scanner = table.getScanner(scan);
	        for (Result result : scanner){
	            for (Cell cell:result.rawCells()){
	                System.out.println(Bytes.toString(cell.getFamily())+":"+Bytes.toString(cell.getQualifier())
	                		+" --> "+Bytes.toString(cell.getValue()));
	            }
	        }
	        scanner.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
