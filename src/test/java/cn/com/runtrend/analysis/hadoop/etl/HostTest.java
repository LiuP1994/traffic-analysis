package cn.com.runtrend.analysis.hadoop.etl;

import cn.com.runtrend.analysis.commons.Constants;
import cn.com.runtrend.analysis.commons.StringUtils;
import cn.com.runtrend.analysis.core.Trie;
import cn.com.runtrend.analysis.core.TrieObject;
import cn.com.runtrend.analysis.core.UnicodeDecode;
import junit.framework.TestCase;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Pattern;

/**
 * 作者：张乔齐 cn.com.runtrend.analysis.hadoop.etl 2017/9/6 简介：<描述本类作用>
 */
public class HostTest extends TestCase {

  Trie.TrieTree tree = new Trie.TrieTree();

  public boolean testMain() throws Exception {
    loadTree();
//    System.out.println("m1"+"  :  "+search("m1"));
//    System.out.println("m2"+"  :  "+search("m2"));
//    System.out.println("m5 note"+"  :  "+search("m5 note"));
    BufferedReader br = new BufferedReader(new InputStreamReader(
//        new FileInputStream("E:\\work\\runtrend\\策反\\临时文件\\check_terminal2.txt")));
        new FileInputStream("C:\\Users\\Administrator\\Desktop\\analysis\\project\\shyjy\\20170908\\static\\")));
    String line;
    boolean t=true;
    while ((line = br.readLine()) != null) {
      String [] tl = line.split("\\t", -1);
      for (int i = 0;i < tl.length - 1; i++){
        if(search(tl[i].trim())){
          return t;//失败
        }
      }
    }return t=false;
  }

  private void loadTree() throws IOException {
    {
//      BufferedReader br = new BufferedReader(new InputStreamReader(
//          new FileInputStream(
//              "E:\\work\\runtrend\\code\\analysis\\project\\shyjy\\20170908\\static\\traffic_bi_static_terminal_detail.txt")));
      BufferedReader br = new BufferedReader(new InputStreamReader(
          new FileInputStream(
              "E:\\work\\runtrend\\code\\analysis\\project\\shyjy\\20170908\\static\\traffic_bi_static_terminal_detail.txt")));
      String line;
      while ((line = br.readLine()) != null) {
        String[] hostDetail = UnicodeDecode.decodeExt(line)
            .split(Constants.FIELDS_TERMINATED, -1);
        String hostType = hostDetail[0];
        String hostName = hostDetail[1];
        String host = hostDetail[2];
        if (tree.search(host).getmString1().equals("")) {
          tree.insert(host, new TrieObject.Builder().mString1(hostType).mString2(hostName).build());
        }
      }
      br.close();
    }
  }

  private Boolean search(String host) {
    boolean flag = false;
    if (!host.equals("")) {
      TrieObject trieObject = tree.search(host);
      if (trieObject.getmString1().equals("")||trieObject.getmString2().equals("")) {
        flag=true;//没匹配到true
        return flag;
      }}
      return flag;//匹配到false

  }
}