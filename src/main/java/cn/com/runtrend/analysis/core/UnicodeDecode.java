package cn.com.runtrend.analysis.core;

/**
 * @Auther: 张乔齐
 * @Description: unicode 解码
 * @Date: 2017/8/22
 * @Modified By:
 */
public class UnicodeDecode {

  public static String decode(String theString) {
    char aChar;
    int len = theString.length();
    StringBuilder outBuffer = new StringBuilder(len);
    for (int x = 0; x < len; ) {
      aChar = theString.charAt(x++);
      if (aChar == '\\') {
        aChar = theString.charAt(x++);
        if (aChar == 'u') {
          int value = 0;
          for (int i = 0; i < 4; i++) {
            aChar = theString.charAt(x++);
            switch (aChar) {
              case '0':
              case '1':
              case '2':
              case '3':
              case '4':
              case '5':
              case '6':
              case '7':
              case '8':
              case '9':
                value = (value << 4) + aChar - '0';
                break;
              case 'a':
              case 'b':
              case 'c':
              case 'd':
              case 'e':
              case 'f':
                value = (value << 4) + 10 + aChar - 'a';
                break;
              case 'A':
              case 'B':
              case 'C':
              case 'D':
              case 'E':
              case 'F':
                value = (value << 4) + 10 + aChar - 'A';
                break;
              default:
                return theString;
            }

          }
          outBuffer.append((char) value);
        } else {
          if (aChar == 't') {
            aChar = '\t';
          } else if (aChar == 'r') {
            aChar = '\r';
          } else if (aChar == 'n') {
            aChar = '\n';
          } else if (aChar == 'f') {
            aChar = '\f';
          }
          outBuffer.append(aChar);
        }
      } else {
        outBuffer.append(aChar);
      }
    }
    return outBuffer.toString();
  }

  public static String decodeExt(String encode) {
    if (encode.contains("\\u")) {
      return findUnicode2Decode(encode, "");
    } else {
      return encode;
    }
  }

  private static String findUnicode2Decode(String text, String newText) {
    int index = text.indexOf("\\u");
    if (index >= 0 && index < text.length() - 1 && index + 6 <= text.length()) {
      if (checkUnicode(text.substring(index, index + 6))) {
        String s1 = newText + text.substring(0, index);
        int end = findUnicode(text, index);
        String s2 = s1 + decode((text.substring(index, end)));
        return findUnicode2Decode(text.substring(end), s2);
      }
    }
    int temp = text.substring(index + 1).indexOf("\\u");
    if (temp >= 0) {
      return findUnicode2Decode(text.substring(temp + index + 1),
          newText + text.substring(0, temp + index + 1));
    }

    return newText + text;
  }

  private static int findUnicode(String text, int index) {
    if (text.substring(index).length() > 7 && "\\u".equals(text.substring(index + 6, index + 8))) {
      if (checkUnicode(text.substring(index, index + 6))) {
        return findUnicode(text, index + 6);
      } else {
        return index;
      }
    } else {
      if (index + 6 <= text.length() && checkUnicode(text.substring(index, index + 6))) {
        return index + 6;
      } else {
        return index;
      }
    }
  }

  private static boolean checkUnicode(String unicode) {
    char[] enChar = unicode.toCharArray();
    if (enChar.length == 6) {
      if (enChar[0] == '\\') {
        if (enChar[1] == 'u') {
          for (int i = 2; i < 6; i++) {
            if (!checkOx(enChar[i])) {
              return false;
            }
          }
          return true;
        }
      }
    }
    return false;
  }

  private static boolean checkOx(char c) {
    return (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F') || (c >= '0' && c <= '9');
  }


}
