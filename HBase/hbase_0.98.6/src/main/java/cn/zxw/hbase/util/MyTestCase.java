package cn.zxw.hbase.util;

import junit.framework.TestCase;

public class MyTestCase extends TestCase{
	
	@Override
	protected void setUp() throws Exception {
		
		super.setUp();
	}

	@Override
	protected void tearDown() throws Exception {
		HBaseConnectUtil.close();
		super.tearDown();
	}
}
