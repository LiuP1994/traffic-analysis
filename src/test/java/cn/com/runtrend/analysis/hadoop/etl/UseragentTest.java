package cn.com.runtrend.analysis.hadoop.etl;

import cn.com.runtrend.analysis.commons.StringUtils;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import junit.framework.TestCase;

/**
 * 作者：张乔齐 cn.com.runtrend.analysis.hadoop.etl 2017/8/17 简介：<描述本类作用>
 */
public class UseragentTest extends TestCase {

  public void testIphone() {
    String iphone = "";
    String url = "http://www.baidu.com?dev=iphone6,7";
    if (iphone.equals("")) {
      iphone = StringUtils.regex(".*\\=(iphone[0-9]{1,2},[0-9]{1,2}).*", url);
    }
    if (iphone.equals("")) {
      iphone = StringUtils.regex(".*\\=(ipad[0-9]{1,2},[0-9]{1,2}).*", url);
    }
    if (iphone.equals("")) {
      iphone = StringUtils.regex(".*\\=(ipod[0-9]{1,2},[0-9]{1,2}).*", url);
    }
    System.out.println(iphone);
  }

  public void testMain() throws Exception {
    Map<String, List<String>> regxMap = new HashMap<String, List<String>>();
    BufferedReader br = new BufferedReader(
        new InputStreamReader(
            new FileInputStream("E:\\work\\runtrend\\code\\analysis\\project\\shyjy\\20170908\\static\\traffic_bi_static_ua.txt")));
    String line;
    while ((line = br.readLine()) != null) {

      if (line.startsWith("(") && line.endsWith(")")) {
        String subLine = line.replaceAll("\\(", "")
            .replaceAll("\\)", "");
        List<String> list = new ArrayList<String>();
        regxMap.put(subLine, list);
      }
      String startName = startName(line);

      if (!"".equals(startName)) {
        if (regxMap.containsKey(startName)) {
          List<String> list = regxMap.get(startName);
          list.add(line);
          regxMap.put(startName, list);
        } else {
          List<String> list = new ArrayList<String>();
          list.add(line);
          regxMap.put(startName, list);
        }
      }
    }
    br.close();

//    String ua = "Dalvik/2.1.0 (Linux; U; Android 5.1; m3 note Build/LMY47I)".toLowerCase();
    List<String> uas = new ArrayList<String>();
    uas.add(
        "Mozilla/5.0 (Linux; U; Android 4.4.4; zh-cn; B860AV1.1 Build/KTU84P) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile "
            .toLowerCase());
    uas.add(
        "Mozilla/5.0(Linux;U;Android 2.2.1;en-us;Nexus One Build.FRG83) AppleWebKit/553.1(KHTML,like Gecko) Version/4.0 Mobile Safari/533.1	3748722"
            .toLowerCase());
    uas.add(
        "B700-V2A|Mozilla|5.0|ztebw(Chrome)|1.2.0;Resolution(PAL,720p,1080i) AppleWebKit/535.7 (KHTML, like Gecko) Chrome/16.0.912.63 "
            .toLowerCase());
    uas.add(
        "Mozilla/5.0 (Linux; Android 4.4.2; EC6108V9_pub_hnydx Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/30.0.0.0 "
            .toLowerCase());
    uas.add(
        "Mozilla/5.0 (Linux; U; Android 4.4.4; zh-cn; we30c Build/CV3229_BL_MTK7601) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile "
            .toLowerCase());
    uas.add(
        "Mozilla/5.0 (Linux; U; Android 4.4.2; zh-cn; EC6108V9_pub_hnydx Build/KOT49H) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile "
            .toLowerCase());
    uas.add(
        "Mozilla/5.0 (Linux; U; Android 2.2; zh-cn; Desire_A8181 Build/FRF91) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile "
            .toLowerCase());
    uas.add(
        "AppleCoreMedia/1.0.0.7B367 (iPad; U; CPU OS 4_3_3 like Mac OS X)	1772152".toLowerCase());
    uas.add("IPhone-8800-UserAction	1372097".toLowerCase());
    uas.add("Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)	1358075".toLowerCase());
    uas.add("Dalvik/1.6.0 (Linux; U; Android 4.0.3 Build/BesTV_Mozart_SH_2.6.0.9)	1330277"
        .toLowerCase());
    uas.add(
        "Mozilla/5.0 (Linux; U; Android 2.3.4; en-us; T-Mobile myTouch 3G Slide Build/GRI40) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 "
            .toLowerCase());
    uas.add("NeteaseMusic 4.1.2/844 (iPhone; iOS 10.3.3; zh_CN)	772104".toLowerCase());
    uas.add("Dalvik/2.1.0 (Linux; U; Android 5.1; MiBOX3_PRO Build/LMY47D)	658419".toLowerCase());
    uas.add("Dalvik/2.1.0 (Linux; U; Android 5.1; 1501_M02 Build/LMY47D)	652390".toLowerCase());
    uas.add(
        "Mozilla/5.0 (iPhone; CPU iPhone OS 10_3_3 like Mac OS X; zh-CN) AppleWebKit/537.51.1 (KHTML, like Gecko) Mobile/14G60 UCBrowser/11.5.9.992 "
            .toLowerCase());
    uas.add(
        "com.letv.tv_2.10.1_292-Dalvik/1.6.0 (Linux; U; Android 4.3; Letv S40 Air Build/V2202RCN02C058050B10151S)	572492"
            .toLowerCase());
    uas.add(
        "Mozilla/5.0 (iPhone; CPU iPhone OS 10_3_3 like Mac OS X; zh-CN) AppleWebKit/537.51.1 (KHTML, like Gecko) Mobile/14G60 "
            .toLowerCase());
    uas.add(
        "com.letv.tv_2.10.1_292-Dalvik/2.1.0 (Linux; U; Android 5.0.1; Letv X3-55 Build/V2301RCN02C058052B12312S)	514786"
            .toLowerCase());
    uas.add(
        "com.letv.tv_2.10.1_292-Dalvik/1.6.0 (Linux; U; Android 4.3; Letv S40 Air Build/V2202RCN02C059055B03301S)	494850"
            .toLowerCase());
    uas.add(
        "com.letv.tv_2.10.1_292-Dalvik/1.6.0 (Linux; U; Android 4.3; Letv S50 Air Build/V2202RCN02C059055B03301S)	488717"
            .toLowerCase());
    uas.add(
        "Mozilla/5.0 (iPad; CPU OS 10_3_3 like Mac OS X) AppleWebKit/603.3.8 (KHTML, like Gecko) Version/10.0 Mobile/14G60 Safari/602.1	487374"
            .toLowerCase());
    uas.add("MQQBrowser/7.7.1 (iOS 6; U; CPU like Mac OS X; zh-cn)	486403".toLowerCase());
    uas.add("Dalvik/2.1.0 (Linux; U; Android 5.0 Android; 8H87 G7200 Build/LRX21M)	481963"
        .toLowerCase());
    uas.add(
        "com.letv.tv_2.10.1_292-Dalvik/1.6.0 (Linux; U; Android 4.3; Letv S50 Air Build/V2202RCN02C058050B10151S)	473665"
            .toLowerCase());
    uas.add(
        "com.letv.tv_2.10.1_292-Dalvik/2.1.0 (Linux; U; Android 6.0; X4-55 Build/V2401RCN02C060057B06021S)	447558"
            .toLowerCase());
    uas.add("qqlive4Android/7.1.5  Dalvik (Android 6.0.1;OPPO R9s)	422307".toLowerCase());
    uas.add("AppleCoreMedia/1.0.0.12B440 (iPhone; U; CPU OS 8_1_2 like Mac OS X; zh_cn)	403862"
        .toLowerCase());
    uas.add(
        "Mozilla/5.0 (iPad; CPU OS 10_3_3 like Mac OS X) AppleWebKit/603.3.8 (KHTML, like Gecko) Mobile/14G60	398600"
            .toLowerCase());
    uas.add(
        "com.letv.tv_2.10.1_292-Dalvik/2.1.0 (Linux; U; Android 5.0.1; Letv X3-65 Build/V2301RCN02C058052B12312S)	397033"
            .toLowerCase());
    uas.add("Dalvik/2.1.0 (Linux; U; Android 5.1.1; 长虹智能电视 Build/LMY48P)	394895".toLowerCase());
    uas.add("MQQBrowser/7.7.1 (iOS 6s; U; CPU like Mac OS X; zh-cn)	390048".toLowerCase());
    uas.add(
        "AppleCoreMedia/1.0.0.7B367  (iPad; U; CPU OS 4_3_3 like Mac OS X)	389397".toLowerCase());
    uas.add("live4iphone/19835 CFNetwork/758.5.3 Darwin/15.6.0	385356".toLowerCase());
    uas.add(
        "aliyun-sdk-android/2.2.0/Dalvik/2.1.0 (Linux; U; Android 5.1.1; M1 Build/LMY49F)	382162"
            .toLowerCase());
    uas.add(
        "Mozilla/5.0 (Linux; U; Android 4.4.2; zh-cn; B860AV1.1-T Build/KOT49H) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile "
            .toLowerCase());
    uas.add(
        "Mozilla/5.0 (iPad; CPU OS 10_3_2 like Mac OS X) AppleWebKit/603.2.4 (KHTML, like Gecko) Version/10.0 Mobile/14F89 Safari/602.1	365287"
            .toLowerCase());
    uas.add(
        "com.letv.tv_2.10.1_292-Dalvik/2.1.0 (Linux; U; Android 5.0.1; Letv X3-55 Build/V2301RCN02C058050B10151S)	362807"
            .toLowerCase());
    uas.add("MQQBrowser/7.7.1 (iOS 6p; U; CPU like Mac OS X; zh-cn)	347809".toLowerCase());
    uas.add("MQQBrowser/7.7.1 (iOS 6sp; U; CPU like Mac OS X; zh-cn)	340653".toLowerCase());
    uas.add(
        "com.letv.tv_2.10.1_292-Dalvik/2.1.0 (Linux; U; Android 5.0.1; Letv X3-40S Build/V2301RCN02C058052B12312S)	329909"
            .toLowerCase());
    uas.add(
        "com.letv.tv_2.10.1_292-Dalvik/2.1.0 (Linux; U; Android 6.0; X4-50 Build/V2401RCN02C059055B03281S)	329152"
            .toLowerCase());
    uas.add(
        "com.letv.tv_2.10.1_292-Dalvik/2.1.0 (Linux; U; Android 6.0; X4-43 Build/V2401RCN02C060057B06021S)	306860"
            .toLowerCase());
    uas.add("qqlive4Android/7.1.5  Dalvik (Android 6.0.1;OPPO A57)	303716".toLowerCase());
    uas.add(
        "stagefright/1.2 (Linux;Android 4.0.3) Mozilla/5.0(iPad; U; CPU iPhone OS 3_2 like Mac OS X; en-us) AppleWebKit/531.21.10 (KHTML, like Gecko) Version/4.0.4 Mobile/7B314 Safari/531.21.10 QuickTime	297184"
            .toLowerCase());
    uas.add("MQQBrowser/7.7.1 (iOS 92; U; CPU like Mac OS X; zh-cn)	286798".toLowerCase());
    uas.add(
        "Dalvik/v3.3.63_update5 (Linux; U; Android 6.0.6-RS-20170508.1926; MagicBox_M16C Build/KOT49H)	280254"
            .toLowerCase());
    uas.add(
        "Mozilla/5.0 (iPad; CPU OS 9_3_5 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13G36 Safari/601.1	273080"
            .toLowerCase());
    uas.add(
        "Mozilla/5.0 (iPhone; CPU iPhone OS 10_3_2 like Mac OS X; zh-CN) AppleWebKit/537.51.1 (KHTML, like Gecko) Mobile/14F89 UCBrowser/11.5.9.992 "
            .toLowerCase());
    uas.add(
        "com.letv.tv_2.10.1_292-Dalvik/2.1.0 (Linux; U; Android 5.0.1; Letv X3-40S Build/V2301RCN02C058050B10151S)	257053"
            .toLowerCase());
    uas.add(
        "Mozilla/5.0 (iPad; CPU OS 9_3_5 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Mobile/13G36	255509"
            .toLowerCase());
    uas.add("MQQBrowser/7.7.1 (iOS 91; U; CPU like Mac OS X; zh-cn)	236448".toLowerCase());
    uas.add(
        "Dalvik/v3.3.86_update2 (Linux; U; Android 4.4.4; m1 Build/KTU84P)	233312".toLowerCase());
    uas.add(
        "com.letv.tv_2.10.1_292-Dalvik/2.1.0 (Linux; U; Android 6.0; X4-40 Build/V2401RCN02C060057B06021S)	232371"
            .toLowerCase());
    uas.add("iPhone".toLowerCase());
    uas.add("live4iphone/19835 CFNetwork/758.4.3 Darwin/15.5.0	228722".toLowerCase());
    uas.add("AppleCoreMedia/1.0.0.14G60 (iPhone; U; CPU OS 10_3_3 like Mac OS X; zh_cn)	221576"
        .toLowerCase());
    uas.add("PaiPai 6.0.110 (iPhone; iOS 10.3.3; zh_CN)	211853".toLowerCase());
    uas.add("Dalvik/2.1.0 (Linux; U; Android 6.0.1; GM-Q5+ Build/MMB29M)	209481".toLowerCase());
    uas.add(
        "com.letv.tv_2.10.1_292-Dalvik/1.6.0 (Linux; U; Android 4.3; Letv X50 Air Build/V2201RCN02C059055B03301S)	207625"
            .toLowerCase());
    uas.add(
        "com.letv.tv_2.10.1_292-Dalvik/2.1.0 (Linux; U; Android 6.0; X4-50 Build/V2401RCN02C058050B10151S)	206412"
            .toLowerCase());
    uas.add(
        "com.letv.tv_2.10.1_292-Dalvik/2.1.0 (Linux; U; Android 5.0.1; Letv X3-43S Build/V2301RCN02C058052B12312S)	185447"
            .toLowerCase());
    uas.add(
        "com.letv.tv_2.10.1_292-Dalvik/1.6.0 (Linux; U; Android 4.3; Letv X3-40 Build/V2202RCN02C059055B03301S)	185144"
            .toLowerCase());
    uas.add("oppox903".toLowerCase());
    uas.add("全民K歌 4.0.0 rv:83 (iPhone; iOS 10.3.3; zh_CN)	179321".toLowerCase());
    uas.add(
        "Mozilla/5.0 (Linux; Android 6.0.1; OPPO R9s Build/MMB29M; wv) AppleWebKit/537.36 (KHTML, like Gecko) "
            .toLowerCase());
    uas.add(
        "com.letv.tv_2.10.1_292-Dalvik/2.1.0 (Linux; U; Android 5.0.1; Letv X3-43S Build/V2301RCN02C058050B10151S)	171425"
            .toLowerCase());
    uas.add(
        "Dalvik/2.1.0 (Linux; U; Android 6.0.6-RS-20170424.1818; MagicBox_M13 Build/LMY47V)	168788"
            .toLowerCase());
    uas.add(
        "com.letv.tv_2.10.1_292-Dalvik/2.1.0 (Linux; U; Android 6.0; X4-50Pro Build/V2401RCN02C059055B03281S)	167517"
            .toLowerCase());
    uas.add("高德地图 8.1.2 rv:8.1.2.2127 (iPhone; iOS 10.3.3; zh_CN)	167273".toLowerCase());
    uas.add(
        "Mozilla/5.0 (Linux; Android 5.1; OPPO A37m Build/LMY47I; wv) AppleWebKit/537.36 (KHTML, like Gecko) "
            .toLowerCase());
    uas.add("MQQBrowser/7.7.1 (iOS 5SGLOBAL; U; CPU like Mac OS X; zh-cn)	162883".toLowerCase());
    uas.add(
        "com.letv.tv_2.10.1_292-Dalvik/1.6.0 (Linux; U; Android 4.3; Letv X3-40 Build/V2202RCN02C058050B10151S)	160881"
            .toLowerCase());
    uas.add(
        "com.letv.tv_2.10.1_292-Dalvik/2.1.0 (Linux; U; Android 6.0; X4-43 Build/V2401RCN02C058051B12131S)	157528"
            .toLowerCase());
    uas.add(
        "com.letv.tv_2.10.1_292-Dalvik/1.6.0 (Linux; U; Android 4.3; Letv X3-43 Build/V2202RCN02C058050B10151S)	152205"
            .toLowerCase());
    uas.add("qqlive4Android/7.1.5  Dalvik (Android 5.1;OPPO A37m)	150439".toLowerCase());
    uas.add(
        "com.letv.tv_2.10.1_292-Dalvik/2.1.0 (Linux; U; Android 5.0.1; Letv X3-50 Build/V2301RCN02C058052B12312S)	150177"
            .toLowerCase());
    uas.add(
        "Mozilla/5.0 (Linux; Android 6.0.1; OPPO R9s Build/MMB29M; wv) AppleWebKit/537.36 (KHTML, like Gecko) "
            .toLowerCase());
    uas.add("NeteaseMusic 4.1.2/844 (iPhone; iOS 10.2.1; zh_CN)	146415".toLowerCase());
    uas.add(
        "com.letv.tv_2.10.1_292-Dalvik/1.6.0 (Linux; U; Android 4.3; Letv X3-43 Build/V2202RCN02C059055B03301S)	144933"
            .toLowerCase());
    uas.add(
        "com.letv.tv_2.10.1_292-Dalvik/2.1.0 (Linux; U; Android 6.0; X4-40 Build/V2401RCN02C058051B12131S)	140883"
            .toLowerCase());
    uas.add(
        "Mozilla/5.0 (Linux; Android 5.1; OPPO R9m Build/LMY47I; wv) AppleWebKit/537.36 (KHTML, like Gecko) "
            .toLowerCase());
    uas.add(
        "Mozilla/5.0 (iPhone; CPU iPhone OS 10_2_1 like Mac OS X; zh-CN) AppleWebKit/537.51.1 (KHTML, like Gecko) "
            .toLowerCase());
    uas.add(
        "Mozilla/5.0 (Linux; Android 6.0.1; vivo Y66 Build/MMB29M; wv) AppleWebKit/537.36 (KHTML, like Gecko) "
            .toLowerCase());
    uas.add(
        "Mozilla/5.0 (Linux; Android 5.1; OPPO A59s Build/LMY47I; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/43.0.2357.121 Mobile Safari/537.36	132840"
            .toLowerCase());
    uas.add(
        "Mozilla/5.0 (Linux; Android 6.0.1; vivo X9 Build/MMB29M; wv) AppleWebKit/537.36 (KHTML, like Gecko) "
            .toLowerCase());
    uas.add("qqlive4Android/7.1.5  Dalvik (Android 6.0.1;OPPO R9sk)	127718".toLowerCase());
    uas.add(
        "Dalvik/v3.3.63_update5 (Linux; U; Android 6.0.4-RS-20170320.1534; MagicBox1s_Plus Build/KOT49H)	126223"
            .toLowerCase());
    uas.add(
        "com.letv.tv_2.10.1_292-Dalvik/2.1.0 (Linux; U; Android 6.0; X4-55 Build/V2401RCN02C058051B12131S)	124654"
            .toLowerCase());
    uas.add(
        "Mozilla/5.0 (Linux; Android 5.1; OPPO A37m Build/LMY47I; wv) AppleWebKit/537.36 (KHTML, like Gecko) "
            .toLowerCase());
    uas.add(
        "com.letv.tv_2.10.1_292-Dalvik/2.1.0 (Linux; U; Android 6.0; X4-55 Build/V2401RCN02C058050B10151S)	120667"
            .toLowerCase());
    uas.add(
        "Mozilla/5.0 (Linux; Android 6.0.1; OPPO A57 Build/MMB29M; wv) AppleWebKit/537.36 (KHTML, like Gecko) "
            .toLowerCase());
    uas.add(
        "com.letv.tv_2.10.1_292-Dalvik/2.1.0 (Linux; U; Android 6.0; X4-55 Build/V2401RCN02C059055B03281S)	113160"
            .toLowerCase());
    uas.add(
        "Mozilla/5.0 (Linux; Android 5.1.1; OPPO A33 Build/LMY47V; wv) AppleWebKit/537.36 (KHTML, like Gecko) "
            .toLowerCase());

    for (String ua : uas) {
      String terminal = "";
      if (regxMap.containsKey(ua)){
        terminal = ua;
      }
      String startName = startName(ua);

      if (regxMap.containsKey(startName) && "".equals(terminal)) {
        for (String regex : regxMap.get(startName)) {
          String str = StringUtils.regex(regex, ua);
          if (!str.equals("")) {
            terminal = str;
            break;
          }
        }
      }
      System.out.println(terminal);
    }
  }

  private static String startName(String line) {
    StringBuilder sb = new StringBuilder();
    char[] lineChars = line.toCharArray();
    for (char c : lineChars) {
      if ((c >= '\u4e00' && c <= '\u9fa5') || (c >= 'a' && c <= 'z')) {
        sb.append(c);
      } else {
        break;
      }
    }
    return sb.toString();
  }

  public void testIphone1() {
    String abc = "111\t222";
    String[] ab = abc.split("\t", -1);
    System.out.println(ab[0]);
    System.out.println(ab[1]);
  }

  private String ua(String url){
    url = url.toLowerCase();
    String terminal = "";
    if (terminal.equals("")) {
      if (url.contains("&device_type=")) {
        String tb = StringUtils.regex(".*&device_brand\\=([0-9a-zA-Z]+)", url);
        String tm = StringUtils.regex(".*&device_type\\=([0-9a-zA-Z\\+\\-\\_]+)", url).replaceAll("\\+"," ");
        if (tm.contains(tb)){
          terminal = tm;
        }
      }
    }

    if (terminal.equals("")) {
      if (url.contains("&model=")) {
        String tb = StringUtils.regex(".*&brand\\=([0-9a-zA-Z]+)", url);
        String tm = StringUtils.regex(".*&model\\=([0-9a-zA-Z\\+\\-\\_]+)", url).replaceAll("\\+"," ");
        if (tm.contains(tb)){
          terminal = tm;
        }
      }
    }

    if (terminal.equals("")) {
      if (url.contains("&md=")) {
        String tb = StringUtils.regex(".*&br\\=([0-9a-zA-Z]+)", url);
        String tm = StringUtils.regex(".*&md\\=([0-9a-zA-Z\\+\\-\\_]+)", url).replaceAll("\\+"," ");
        if (tm.contains(tb)){
          terminal = tm;
        }
      }
    }

    if (terminal.equals("")) {
      if (url.contains("&m=")) {
        String tb = StringUtils.regex(".*&b\\=([0-9a-zA-Z]+)", url);
        String tm = StringUtils.regex(".*&m\\=([0-9a-zA-Z\\+\\-\\_]+)", url).replaceAll("\\+"," ");
        if (tm.contains(tb)){
          terminal = tm;
        }
      }
    }

    if (terminal.equals("")) {
      if (url.contains("&mdl=")) {
        String tb = StringUtils.regex(".*&bd\\=([0-9a-zA-Z]+)", url);
        String tm = StringUtils.regex(".*&mdl\\=([0-9a-zA-Z\\+\\-\\_]+)", url).replaceAll("\\+"," ");
        if (tm.contains(tb)){
          terminal = tm;
        }
      }
    }

    if (terminal.equals("")) {
      if (url.contains("\"c1\":")) {
        String tb = StringUtils.regex(".*\"c0\":\"([0-9a-zA-Z]+)\"", url);
        String tm = StringUtils.regex(".*\"c1\":\"([0-9a-zA-Z\\+\\-\\_]+)\"", url).replaceAll("\\+"," ");
        if (tm.contains(tb)){
          terminal = tm;
        }
      }
    }
    if (terminal.equals("")) {
      if (url.contains("\"pm\":")) {
        String tb = StringUtils.regex(".*\"br\":\"([0-9a-zA-Z]+)\"", url);
        String tm = StringUtils.regex(".*\"pm\":\"([0-9a-zA-Z\\+\\-\\_]+)\"", url).replaceAll("\\+"," ");
        if (tm.contains(tb)){
          terminal = tm;
        }
      }
    }

    if (terminal.equals("")) {
      if (url.contains("&systemmodel=")) {
        String tb = StringUtils.regex(".*&systemphone\\=([0-9a-zA-Z]+)", url);
        String tm = StringUtils.regex(".*&systemmodel\\=([0-9a-zA-Z\\+\\-\\_]+)", url).replaceAll("\\+"," ");
        if (tb.equals("android")){
          terminal = tm;
        }
      }
    }

    if (terminal.equals("")) {
      if (url.contains("\"device\":")) {
        String tb = StringUtils.regex(".*\"device\":\"([0-9a-zA-Z]+)", url);
        String tm = StringUtils.regex(".*\"device\":\"[0-9a-zA-Z]+\\+([0-9a-zA-Z\\+\\-\\_]+)", url).replaceAll("\\+"," ");
        if (tm.contains(tb)){
          terminal = tm;
        }
      }
    }

    if (terminal.equals("")) {
      if (url.contains("&model=") || url.contains("?model=")) {
        terminal = StringUtils.regex(".*[&\\?]model\\=([0-9a-zA-Z]+\\+[0-9a-zA-Z\\-\\_]+)", url).replaceAll("\\+"," ");
      }
    }

    if (terminal.equals("")) {
      if (url.contains("&p16=")) {
        terminal = StringUtils.regex(".*&p16\\=([0-9a-zA-Z]+\\+[0-9a-zA-Z\\-\\_]+)", url).replaceAll("\\+"," ");
      }
    }

    if (terminal.equals("")) {
      if (url.contains("&dev_ua=")) {
        terminal = StringUtils.regex(".*&dev_ua\\=([0-9a-zA-Z]+\\+[0-9a-zA-Z\\-\\_]+)", url).replaceAll("\\+"," ");
      }
    }

    if (terminal.equals("")) {
      if (url.contains("&m_ver=")) {
        terminal = StringUtils.regex(".*&m_ver\\=([0-9a-zA-Z]+\\+[0-9a-zA-Z\\-\\_]+)", url).replaceAll("\\+"," ");
      }
    }
    return terminal;
  }

  public void testUaUrl(){
    System.out.println(ua("http://140.205.203.91:8007/rest/api3.do?ttid=702307@taobao_android_4.2.0&t=1508114849&deviceId=AltmD2Rl-rs1e_ScoFZj3Kmjm_HkK0JAeP2KWq4tMFBn&imei=357458044256062&appKey=12278902&v=6.0&sign=03726cfe319d3a3464184d322faf501c&data={\"c1\":\"HUAWEI+P7-L00\",\"c\n"
        + "2\":\"357458044256062\",\"vote\":\"remote\",\"agooSdkVersion\":20140301,\"c0\":\"Huawei\",\"c6\":\"faaf2b73e39906bf\",\"c5\":\"7N2MYN14CD027801\",\"c4\":\"e0:19:1d:3e:08:23\",\"c3\":\"460016411006857\",\"appPackage\":\"com.taobao.taobao\",\"deviceId\":\"AltmD2Rl-rs1e_ScoFZj3Kmjm_HkK0JA\n"
        + "eP2KWq4tMFBn\"}&api=mtop.push.msg.new&imsi=460016390035261&nt=wifi&lac=28963&cid=25"));
    System.out.println(ua("http://www.51creditapi.com//zxbbs/getUrl.action?OSDEVICEID=864129033022890&OSVERSION=7.0&a_or_ios=A&appName=zhengxindaikuan&area=孝感&areaId=0&channel=yybka&cityId=149&clientIdentify=864129033022890&lat=31.405253&lng=114.334502&systemModel=HUAWEI+MLA"
        + "-AL10&systemPhone=android&type=url_type_490&userid=10477052&version_a=4.0.1&zxSign=9a8cc5b08cd9f11eb4763b"));
    System.out.println(ua("http://input.shouji.sogou.com/SogouServlet?cmd=recommendpapaya&duid=863167012021393&aid=6871f58cd90e2b65&mac=24:DB:AC:A2:B5:87&ts=1508115217&d={\"sn\":\"\",\"ll\":\"zho\",\"sm\":1,\"so\":\"46002\",\"yy\":0,\"br\":\"HUAWEI\",\"xx\":0,\"av\":\"2.3.7\",\"mc\":false,\"pm\":\"HUAWEI+T8"
        + "828\",\"lc\":\"CHN\",\"pn\":\"\",\"mf\":\"HUAWEI\",\"dn\":\"\",\"wc\":true,\""));
    System.out.println(ua("tp://cupdate.client.189.cn:8006/ClientUpdate/services/clientInfo/version?reqParam={\"mobile\":\"13307211859\",\"company\":\"F\",\"device\":\"HUAWEI+HUAWEI+RIO-AL00\",\"currentVersion\":\"6.0.3\",\"reqTime\":\"20171016090517885\"}     HUAWEI HUAWEI RIO-AL00/6.0.3"));
    System.out.println(ua("http://sdksp.video.qq.com/getmfomat?model=HUAWEI+G7-UL20&network_type=1&sysver=4.4.4&install_time=1504006675&mac=0c:d6:bd:b1:28:fa&uin=&submodel=&height=1184&cpuname=Qualcomm+Technologies,+Inc+MSM8916&platform=aphone&cpufreq=1209&qqlog=&player_channe\n"
        + "l_id=248&imei=863846026595068&width=720&device_id=88c1738483260269&osver=4.4.4&numofcpucore=4&appver=V4.2.248.0062&guid=88c1738483260269&cmd=get_android_fomat&randnum=0.3321647678129047&market_id=-1&cpuarch=6&imsi=460002903766869&otype=json        qq\n"
        + "live4Android/7.1.8  Dalvik (Android 4.4.4;HUAWEI G7-UL20)"));
    System.out.println(ua("http://apk.book.3g.cn/apkinfo/BookRecommend/getAddsignBook?packagename=com.jiubang.bookv4&ggid=C059C5E1EDFEB6C7F6DFE8951915B85A&pid=618&imei=869598027413718&mid=7b5d229ff2d0beb&pass=3c5ae4893792727038175291408cdfdd&versioncode=55&versionname=5.3.2&s_\n"
        + "ver=6.0.1&m_ver=HUAWEI+RIO-AL00 /Android/6.0.1/HUAWEI RIO-AL00/zh_CN"));
    System.out.println(ua("http://118.190.123.26/staction?nodown=0&sr=1196*720&apk_pkg=com.gzyr.atkp.huawei&ovc=22&ui=460003720988432&da=862772031862908&n=wifi&ch=t.yxjd&msg=search+app+fail&yxjd_ch=40005120765&ec=254867713&scl=860730031781488&m=HUAWEI+TAG-AL00&l=zh&b=HUAWEI&c=\n"
        + "1&id=uid_286_551472&avn=1.2&err=1&en=G&ovn=5.1&avc=20&r=HUAWEITAG-AL00&el=28817"));
    System.out.println(ua("http://service.suannihaoyun.com/maction?ovc=22&apk_pkg=com.gzyr.atkp.huawei&da=862772031862908&ui=460003720988432&n=wifi&ch=t.yxjd&yxjd_ch=40005120765&ec=254867713&m=HUAWEI+TAG-AL00&l=zh&b=HUAWEI&c=CN&avn=1.2&id=5&en=G&ovn=5.1&ac=complete&r=HUAWEITAG\n"
        + "-AL00&avc=20&el=28817"));
    System.out.println(ua("http://i/adv/m?osv=7.0&vt=0&guid=a7ca9fb93af88d701f835e0e0aa3a8e6&site=1&utdid=VuzdT2HtG1ADABIvXw1o4yp1&fu=0&avs=6.11.3&sver=4.1.7&vc=0&_t_=1508116181&v=XMjM2MzMyMTg0MA==&sid=d19fd5fc9652f6dce1008e31d3820ef1&ft=0&p=23&im=862209038783753&wintype=mdevi\n"
        + "ce&ss=5.0&isp=中国电信_46003&aaid=&pid=642a656c40601971&mdl=HUAWEI+CAZ-AL10&ps=-1&mac=b0:89:00:ab:5c:93&aw=a&dvw=1080&dvh=1920&net=1000&rst=flv&bd=HUAWEI&os=Android&pt=0.0&isvert=0&bt=phone&ua=Mozilla/5.0+(Linux;+Android+7.0;+HUAWEI+CAZ-AL10+Build/HU\n"
        + "AWEICAZ-AL10;+wv)+AppleWebKit/537.36+(KHTML,+like+Gecko)+Version/4.0+Chrome/43.0.2357.65+Mobile+Safari/537.36&vs=1.0&dprm=3000&closed=0&aid=ea9a0e41ee485313    Youku;6.11.3;Android;7.0;H"));
  }
}