package cn.com.runtrend.analysis.commons;

import java.io.UnsupportedEncodingException;
import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

/**
 * @Auther: 张乔齐
 * @Description: Base64加密解密
 * @Date: 2017/1/5
 * @Modified By:
 */
public class Base64 {

  // 加密
  public static String getBase64(String str) {
    byte[] b = null;
    String s = null;
    try {
      b = str.getBytes("utf-8");
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    if (b != null) {
      s = new BASE64Encoder().encode(b);
    }
    return s;
  }

  // 解密
  public static String getFromBase64(String s) {
    byte[] b = null;
    String result = null;
    if (s != null) {
      BASE64Decoder decoder = new BASE64Decoder();
      try {
        b = decoder.decodeBuffer(s);
        result = new String(b, "utf-8");
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return result;
  }
}
