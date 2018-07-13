package cn.zxw.hbase.put.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import cn.zxw.hbase.put.PutOpera;

public class PrepareData {

	public static void main(String[] args) {
		try {
			PutOpera opera=new PutOpera();
			Map<String, String> values=new HashMap<String, String>();
			
			values.put("k102", "102");
			values.put("k103", "103");
			values.put("k104", "104");
			values.put("k105", "105");
			values.put("k106", "106");
			opera.writeRow("test", "row1", "cf", values);
			values.put("k101", "101");
			opera.writeRow("test", "row2", "cf", values);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
