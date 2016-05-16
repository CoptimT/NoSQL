package cn.zxw.hbase.put;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import cn.zxw.hbase.constant.Constants;
import cn.zxw.hbase.util.HBaseConnectUtil;
import cn.zxw.hbase.util.MyTestCase;

public class PutOpera extends MyTestCase{
	
	public void testPut(){
		String rowKey = Constants.ROW_KEY_PRE + "001";
		Map<String,String> values = new HashMap<String, String>();
		values.put(Constants.COL_USER, "zhangsan");
		values.put(Constants.COL_PASS, "123456");
		try {
			writeRow(Constants.TABLE_NAME,rowKey,Constants.CF_DEFAULT,values);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 插入数据行
	 * @param tableName
	 * @param rowKey
	 * @param cf
	 * @param values
	 * @throws IOException
	 */
	public void writeRow(String tableName,String rowKey,String cf,Map<String,String> values) throws IOException {
		Put put=new Put(Bytes.toBytes(rowKey));
		for(String key:values.keySet()){
			put.add(Bytes.toBytes(cf), Bytes.toBytes(key), Bytes.toBytes(values.get(key)));
		}
		HTable table=HBaseConnectUtil.getTable(tableName);
		table.put(put);
		System.out.println("插入数据行成功！");
	}
	
}
