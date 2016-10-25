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
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class ColumnValueFilter {
	public static Configuration configuration;
	
	static {
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		configuration.set("hbase.zookeeper.quorum", "10.10.25.13,10.10.25.14,10.10.25.15");
		configuration.set("dfs.client.socket-timeout", "18000");
		configuration.set("hbase.client.retries.number", "3");//失败时重试次数,默认为32次
		configuration.set("hbase.client.pause", "50");//失败重试时等待时间,默认值为100ms,可以设置为50ms
		configuration.set("hbase.rpc.timeout", "2000");//一次RPC请求的超时时间,默认该值为1min,不建议修改
		configuration.set("hbase.client.operation.timeout", "3000");//客户端发起一次数据操作直至得到响应之间总的超时时间
		configuration.set("hbase.client.scanner.timeout.period", "10000");//客户端发起一次scan操作的rpc调用至得到响应之间总的超时时间
	}
	
	@SuppressWarnings("unused")
	public static void main(String[] args) throws Exception {
		HTable table = null;
		ResultScanner rs = null;
		String tableName = "users";
		byte[] cf = Bytes.toBytes("info");
		byte[] column = Bytes.toBytes("name");
		byte[] gender = Bytes.toBytes("gender");
		
		try {
			table = new HTable(configuration, tableName);
			Scan scan = new Scan();//new Scan(startRow,stopRow);
			//scan.setBatch(30);
			//scan.addFamily(cf);
			//scan.addColumn(cf, column);
			
			//1.FilterList and/or | SingleColumnValueFilter
			FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE);
			SingleColumnValueFilter filter1 = new SingleColumnValueFilter(cf,column,CompareOp.EQUAL,Bytes.toBytes("Jack"));
			SingleColumnValueFilter filter2 = new SingleColumnValueFilter(cf,column,CompareOp.EQUAL,Bytes.toBytes("zhaoliu"));
			list.addFilter(filter1);
			list.addFilter(filter2);
			//scan.setFilter(list);
			
			//2.RegexStringComparator
			RegexStringComparator comp1 = new RegexStringComparator("zh.");//any value that starts with 'zh'
			SingleColumnValueFilter filter3 = new SingleColumnValueFilter(cf,column,CompareOp.EQUAL,comp1);
			filter3.setFilterIfMissing(false);//如果列本身就不存在,那么设为true,行将会被过滤掉,否则保留
			filter3.setLatestVersionOnly(true);
			//scan.setFilter(filter3);
			
			//3.SubstringComparator
			SubstringComparator comp2 = new SubstringComparator("ang");//looking for 'ang'
			SingleColumnValueFilter filter4 = new SingleColumnValueFilter(cf,column,CompareOp.EQUAL,comp2);
			//scan.setFilter(filter4);
			
			//4.BinaryPrefixComparator
			BinaryPrefixComparator comp5 = new BinaryPrefixComparator(Bytes.toBytes("li"));//any value that starts with 'li'
			SingleColumnValueFilter filter5 = new SingleColumnValueFilter(cf,column,CompareOp.EQUAL,comp5);
			//scan.setFilter(filter5);
			
			//5.BinaryComparator
			BinaryComparator comp6 = new BinaryComparator(Bytes.toBytes("lisi"));//any value that equal 'lisi'
			SingleColumnValueFilter filter6 = new SingleColumnValueFilter(cf,column,CompareOp.EQUAL,comp6);
			//scan.setFilter(filter6);
			
			//所有列值匹配
			Filter filter7 = new ValueFilter(CompareOp.EQUAL, new SubstringComparator("a"));
			//scan.setFilter(filter7);
			
			//与SingleColumnValueFilter唯一的区别就是，作为筛选条件的列的不会包含在返回的结果中
			Filter filter8 = new SingleColumnValueExcludeFilter(cf,gender,CompareOp.EQUAL,new SubstringComparator("M"));
			scan.setFilter(filter8);
			
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
