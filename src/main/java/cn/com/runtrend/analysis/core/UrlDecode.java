package cn.com.runtrend.analysis.core;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.regex.Pattern;

/**
 * @Auther: 张乔齐
 * @Description: 不断迭代流量数据，直到 urlencode 码全部解析
 * @Date: 2017/8/9
 * @Modified By:
 */
public class UrlDecode {

  public static String decode(String encode, String enc) {
    encode = encode.replaceAll("%u", "\\\\u");
    if (checkEncodeRegx(encode)) {
      while (encode.contains("%25")) {
        encode = encode.replaceAll("%25", "%");
      }
      while (encode.contains("%%")) {
        encode = encode.replaceAll("%%", "%");
      }
      encode = decode(encode, enc, "");
      return encode.replaceAll("\\s", "").replaceAll("\t", "")
          .replaceAll("\r\n", "").replaceAll("\n", "");
    } else {
      return encode;
    }
  }

  private static String decode(String text, String enc, String newText) {
    int index = text.indexOf("%");
    if (index >= 0 && index + 3 <= text.length()) {
      if (checkEncode(text.substring(index, index + 3))) {
        String s1 = newText + text.substring(0, index);
        int end = findEncode(text, index);
        String s2 = s1 + urlDecode((text.substring(index, end)), enc);
        return decode(text.substring(end), enc, s2);
      }
    }
    int temp = text.substring(index + 1).indexOf("%");
    if (temp >= 0) {
      return decode(text.substring(temp + index + 1), enc,
          newText + text.substring(0, temp + index + 1));
    }
    return newText + text;
  }

  private static int findEncode(String text, int index) {
    if (text.substring(index).length() > 3 && text.substring(index).charAt(3) == '%') {
      if (checkEncode(text.substring(index, index + 3))) {
        return findEncode(text, index + 3);
      } else {
        return index;
      }
    } else {
      if (index + 3 <= text.length() && checkEncode(text.substring(index, index + 3))) {
        return index + 3;
      } else {
        return index;
      }
    }
  }

  public static boolean checkEncodeRegx(String encode) {
    String pattern = ".*%[0-9a-fA-F]{2}.*";
    return Pattern.matches(pattern, encode);
  }

  private static boolean checkEncode(String encode) {
    char[] enChar = encode.toCharArray();
    if (enChar.length == 3) {
      if (enChar[0] == '%') {
        if (checkOx(enChar[1])) {
          if (checkOx(enChar[2])) {
            return true;
          }
        }
      }
    }
    return false;
  }

  private static boolean checkOx(char c) {
    return (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F') || (c >= '0' && c <= '9');
  }

  private static String urlDecode(String encode, String enc) {
    try {
      return URLDecoder.decode(encode, enc);
    } catch (UnsupportedEncodingException e) {
      return encode;
    }
  }

  public static void main(String[] args) {
    System.out.println(decode("%7B%7D","UTF-8"));
  }
}