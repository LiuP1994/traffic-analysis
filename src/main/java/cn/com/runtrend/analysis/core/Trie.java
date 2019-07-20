package cn.com.runtrend.analysis.core;

import java.util.LinkedList;
import java.util.List;

/**
 * @Auther: 张乔齐
 * @Description: trie 树 优化匹配效率
 * @Date: 2017/8/18
 * @Modified By:
 */
public class Trie {

  static class Node {

    char c;
    boolean isEnd;
    List<Node> childList;
    TrieObject trieObject;

    Node(char c) {
      super();
      this.c = c;
      isEnd = false;
      childList = new LinkedList<Node>();
      trieObject = new TrieObject.Builder().build();
    }

    Node findNode(char c) {
      for (Node node : childList) {
        if (node.c == c) {
          return node;
        }
      }
      return null;
    }
  }

  public static class TrieTree {

    Node root = new Node(' ');

    public void insert(String words, TrieObject trieObject) {
      char[] arr = words.toCharArray();
      Node currentNode = root;
      for (char c : arr) {
        Node node = currentNode.findNode(c);
        if (node == null) {
          Node n = new Node(c);
          currentNode.childList.add(n);
          currentNode = n;
        } else {
          currentNode = node;
        }
      }
      currentNode.isEnd = true;
      currentNode.trieObject = trieObject;
    }

    public TrieObject search(String word) {
      char[] arr = word.toCharArray();
      Node currentNode = root;
      for (int i = 0; i < arr.length; i++) {
        Node n = currentNode.findNode(arr[i]);
        if (n != null) {
          currentNode = n;
          if (n.isEnd) {
            if (n.c == arr[arr.length - 1] && i == arr.length - 1) {
              return n.trieObject;
            }
          }
        } else {
          return new TrieObject.Builder().build();
        }
      }
      return new TrieObject.Builder().build();
    }
  }
}
