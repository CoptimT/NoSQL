package cn.zxw.hbase.get;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import cn.zxw.hbase.constant.Constants;
import cn.zxw.hbase.util.HBaseConnectUtil;
import cn.zxw.hbase.util.MyTestCase;

public class ScanOpera extends MyTestCase{
	
	public void testScan(){
		try {
			scan(Constants.TABLE_NAME,Constants.ROW_KEY_001,Constants.ROW_KEY_PRE+100,Constants.CF_DEFAULT,Constants.COL_USER);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void scan(String tableName, String startRow, String stopRow, String family, String qualifier) throws IOException {
		HTable table = HBaseConnectUtil.getTable(tableName);
		Scan scan = new Scan();
		scan.setCaching(100);
		scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		//scan.setRowPrefixFilter(Bytes.toBytes(ROW_KEY));
		scan.setStartRow(Bytes.toBytes(startRow)); // start key is inclusive
		scan.setStopRow(Bytes.toBytes(stopRow));   // stop key is exclusive
		ResultScanner rs = table.getScanner(scan);
		try {
		  for (Result r = rs.next(); r != null; r = rs.next()) {
			  byte[] bs = r.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
			  System.out.println(new String(bs));
		  }
		} finally {
		  rs.close();//always close the ResultScanner!
		}
	}

}
