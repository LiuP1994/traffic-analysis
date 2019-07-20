package cn.com.runtrend.analysis.commons;

import cn.com.runtrend.analysis.core.Trie;
import cn.com.runtrend.analysis.core.TrieObject;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;

/**
 * @Auther: 张乔齐
 * @Description: 获取域名
 * @Date: 2017/8/22
 * @Modified By:
 */
public class Tld {

  public static List<String> getTlds(String url) {
    LinkedList<String> result = new LinkedList<String>();
    url = url.toLowerCase();
    if (!url.startsWith("http://") && !url.startsWith("https://")) {
      url = "https://" + url;
    }
    Trie.TrieTree tldNames = Domains.INSTANCE.getTldNames();
    String domainName;
    try {
      domainName = new URL(url).getHost();
    } catch (MalformedURLException e) {
      return result;
    }
    if (domainName.equals("")) {
      return result;
    }

    String[] domainParts = domainName.split("\\.");

    String root = "";
    for (int i = domainParts.length - 1; i >= 0; i--) {
      String tmp;
      if (i == domainParts.length - 1) {
        tmp = domainParts[i];
      } else {
        tmp = domainParts[i] + "." + root;
      }
      root = tmp;
      TrieObject trieObject = tldNames.search(root);
      if (trieObject.getmInt1() == -1) {
        if (i > 0) {
          result.addFirst(root);
          result.addFirst("." + root);
        } else {
          result.addFirst(root);
        }
      }
    }
    return result;
  }
}

enum Domains {
  INSTANCE;
  private Trie.TrieTree tree = new Trie.TrieTree();

  private void treeInit() {
    BufferedReader br = new BufferedReader(
        new InputStreamReader(Tld.class.getResourceAsStream(PropertiesParser.getValue("TLD"))));
    String line;
    try {
      while ((line = br.readLine()) != null) {
        if (!line.startsWith("//")) {
          tree.insert(line, new TrieObject.Builder().mInt1(1).build());
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    try {
      br.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    tree.insert("isEmpty-fuck-off", new TrieObject.Builder().mInt1(1).build());
  }

  public Trie.TrieTree getTldNames() {
    TrieObject trieObject = tree.search("isEmpty-fuck-off");
    if (trieObject.getmInt1() > -1) {
      return tree;
    } else {
      treeInit();
      return tree;
    }
  }

  public static void main(String[] args) {
    System.out.println(Tld.getTlds(""));
  }
}