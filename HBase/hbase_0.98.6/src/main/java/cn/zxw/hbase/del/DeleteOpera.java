package cn.zxw.hbase.del;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import cn.zxw.hbase.constant.Constants;
import cn.zxw.hbase.util.HBaseConnectUtil;
import cn.zxw.hbase.util.MyTestCase;

public class DeleteOpera extends MyTestCase{
	
	public void testRow(){
		try {
			deleteRow(Constants.TABLE_NAME,Constants.ROW_KEY_001);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void deleteRow(String tablename, String rowKey) throws IOException {
		Delete delete=new Delete(Bytes.toBytes(rowKey));
		HTable table=HBaseConnectUtil.getTable(tablename);
		table.delete(delete);
		//table.delete(Arrays.asList(delete));
		System.out.println("删除数据行成功！");
	}
	
	public void deleteCFs(String tableName, String rowKey,String... cfs) throws IOException {
		Delete delete=new Delete(Bytes.toBytes(rowKey));
		for(String cf:cfs){
			delete.deleteFamily(Bytes.toBytes(cf));
		}
		HTable table=HBaseConnectUtil.getTable(tableName);
		table.delete(delete);
		//table.delete(Arrays.asList(delete));
		System.out.println("删除行列簇数据成功！");
	}
	
	public void deleteCols(String tableName, String rowKey, String cf, String col) throws IOException {
		Delete delete=new Delete(Bytes.toBytes(rowKey));
		delete.deleteColumn(Bytes.toBytes(cf), Bytes.toBytes(col));
		
		HTable table=HBaseConnectUtil.getTable(tableName);
		table.delete(delete);
		//table.delete(Arrays.asList(delete));
		System.out.println("删除数据列成功！");
	}
}
