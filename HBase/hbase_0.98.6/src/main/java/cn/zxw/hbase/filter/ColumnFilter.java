package cn.zxw.hbase.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class ColumnFilter {
	public static Configuration configuration;
	
	static {
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		configuration.set("hbase.zookeeper.quorum", "10.10.25.13,10.10.25.14,10.10.25.15");
	}
	
	@SuppressWarnings("unused")
	public static void main(String[] args) throws Exception {
		HTable table = null;
		ResultScanner rs = null;
		String tableName = "users";
		byte[] cf = Bytes.toBytes("info");
		byte[] column = Bytes.toBytes("name");
		
		try {
			table = new HTable(configuration, tableName);
			Scan scan = new Scan();//new Scan(startRow,stopRow);
			scan.setBatch(30);
			//scan.addFamily(cf);
			//scan.addColumn(cf, column);
			
			//7.QualifierFilter
			//If an already known column qualifier is looked for, use Get.addColumn directly rather than a filter.
			BinaryComparator comp8 = new BinaryComparator(Bytes.toBytes("name"));//Qualifier that equal 'name'
			QualifierFilter filter8 = new QualifierFilter(CompareOp.EQUAL,comp8);
			//scan.setFilter(filter8);
			//WhileMatchFilter/SkipFilter
			
			//8.ColumnPrefixFilter
			ColumnPrefixFilter filter9 = new ColumnPrefixFilter(Bytes.toBytes("c"));
			//scan.setFilter(filter9);
			
			//9.MultipleColumnPrefixFilter
			MultipleColumnPrefixFilter filter10 = new MultipleColumnPrefixFilter(new byte[][]{Bytes.toBytes("c"),Bytes.toBytes("a")});
			//scan.setFilter(filter10);
			
			//ColumnRangeFilter
			ColumnRangeFilter filter11 = new ColumnRangeFilter(Bytes.toBytes("a"),true,Bytes.toBytes("d"),true);
			//scan.setFilter(filter11);
			
			
			rs = table.getScanner(scan);
			for (Result r = rs.next(); r != null; r = rs.next()) {
				System.out.print(new String(r.getRow()) + "  ");
				for (Cell cell : r.listCells()) {
				    System.out.print(Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
				    		+ Bytes.toString(CellUtil.cloneQualifier(cell)) + "="
				    		+ Bytes.toString(CellUtil.cloneValue(cell)) + "\t");
				}
				System.out.println();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			if(rs != null){
				rs.close();
			}
			if(table != null){
				table.close();
			}
		}
	}

}
