package cn.com.runtrend.analysis.core;

import junit.framework.TestCase;

/**
 * @Auther: 张乔齐
 * @Description:
 * @Date: 2017/9/13
 * @Modified By:
 */
public class UnicodeDecodeTest extends TestCase {

  public void testDecodeExt() throws Exception {
    System.out.println(UnicodeDecode.decodeExt("这是\t正常的"));
  }

}