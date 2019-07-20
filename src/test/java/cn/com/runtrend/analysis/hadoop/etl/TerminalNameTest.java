package cn.com.runtrend.analysis.hadoop.etl;

import cn.com.runtrend.analysis.commons.Constants;
import cn.com.runtrend.analysis.commons.StringUtils;
import cn.com.runtrend.analysis.core.Trie;
import cn.com.runtrend.analysis.core.TrieObject;
import cn.com.runtrend.analysis.core.UnicodeDecode;
import junit.framework.TestCase;

import java.io.*;
import java.util.regex.Pattern;

/**
 * 作者：张乔齐 cn.com.runtrend.analysis.hadoop.etl 2017/9/6 简介：<描述本类作用>
 */
public class TerminalNameTest extends TestCase {

    Trie.TrieTree tree = new Trie.TrieTree();
    Trie.TrieTree iphoneTree = new Trie.TrieTree();

    public void testMain() throws Exception {
        loadTree();
        BufferedReader br = new BufferedReader(new InputStreamReader(
                new FileInputStream("C:\\Users\\zhang\\Desktop\\new 16.txt")));
        String line;
        while ((line = br.readLine()) != null) {
            String[] tl = line.split("\\s", -1);
            String t = "";
            for (int i = 0; i < tl.length - 1; i++) {
                if (!tl[i].equals("")) {
                    t = t + " " + tl[i];
                }
            }
            System.out.println(t + "  :  " + search(t.trim()));
        }
        br.close();
    }

    private void loadTree() throws IOException {
        {
            BufferedReader br = new BufferedReader(new InputStreamReader(
                    new FileInputStream(
                            "E:\\work\\runtrend\\code\\analysis\\project\\shyjy\\20170908\\static\\traffic_bi_static_terminal_detail.txt")));
            String line;
            while ((line = br.readLine()) != null) {
                String[] terminalDetail = UnicodeDecode.decodeExt(line)
                        .split(Constants.FIELDS_TERMINATED, -1);
                String terminal1 = terminalDetail[3];
                String terminal2 = terminalDetail[4];
                addTree("[\u4e00-\u9fa5]+([a-zA-Z0-9\\s\\-\\_\\+\\.]+).*", terminal1, terminal1);
                addTree("([\u4e00-\u9fa5]+[a-zA-Z0-9\\s\\-\\_\\+\\.]+).*", terminal1, terminal1);
                addTree("([a-zA-Z0-9\\s\\-\\_\\+\\.]+).*", terminal1, terminal1);
                addTree(".*（([a-zA-Z0-9\\-\\s\\_\\+\\.]+).*", terminal1, terminal1);
                addTree(".*/([a-zA-Z0-9\\-\\s\\_\\+\\.]+).*", terminal1, terminal1);
                String[] tmnls = terminal2.split(",");
                for (String tmnl : tmnls) {
                    addTree("^[\u4e00-\u9fa5]+([a-zA-Z0-9\\s\\-\\_\\+\\.]+)$", tmnl, terminal1);
                    addTree("^([a-zA-Z0-9\\-\\s\\_\\+\\.]+)$", tmnl, terminal1);
                    addTree("^([0-9]+)$", tmnl, terminal1);
                }
            }
            br.close();
        }
        {
            BufferedReader br = new BufferedReader(new InputStreamReader(
                    new FileInputStream(
                            "E:\\work\\runtrend\\code\\analysis\\project\\test\\static\\traffic_bi_id_iphone")));
            String line;
            while ((line = br.readLine()) != null) {
                String[] terminalDetail = line.split("=", -1);
                String terminal1 = terminalDetail[0];
                String terminal2 = terminalDetail[1];
                iphoneTree.insert(terminal1, new TrieObject.Builder().mString1(terminal2).build());
            }
            br.close();
        }
    }

    private void addTree(String regx, String terminal, String terminalName) {
        String tmnl = StringUtils.regex(regx, terminal).trim().toLowerCase();
        if (!tmnl.equals("") && !tmnl.equals("null")) {
            if (tmnl.length() >= 3) {
                if (!Pattern.matches("^[0-9]+gb\\s.*$", tmnl) && !Pattern.matches("^.*[0-9]+gb$", tmnl) && !Pattern.matches("^[0-9]+gb$", tmnl)) {
                    if (tree.search(tmnl).getmString1().equals("")) {
//            System.out.println(tmnl);
                        tree.insert(tmnl, new TrieObject.Builder().mString1(terminalName).build());
                    }
                }
            }
        }
    }

    private String search(String terminal) {
        terminal = filterTerminal(terminal);
        if (!terminal.equals("") && terminal.length() > 2) {
            boolean flag = false;
            TrieObject trieObject = iphoneTree.search(terminal);
            if (!trieObject.getmString1().equals("")) {
                flag = true;
                return trieObject.getmString1();
            }
            if (!flag) {
                TrieObject trieObject2 = tree.search(terminal);
                return trieObject2.getmString1();
            }
        }
        return "";
    }

    private String filterTerminal(String terminal) {
        String filterTerminal = terminal;
        if (terminal.contains("xiaomi")) {
            filterTerminal = terminal.replace("xiaomi", "");
        }

        if (terminal.equals("pad")) {
            filterTerminal = "";
        }
        if (Pattern.compile("^m[0-9]{1,2}$").matcher(filterTerminal).find()) {
            if (terminal.equals("m1")) {
                filterTerminal = "m456m";
            } else {
                filterTerminal = terminal.replace("m", "魅族魅蓝");
            }
        }

        if (terminal.startsWith("konka android tv")) {
            filterTerminal = "konka android tv";
        }
        if (terminal.startsWith("android tv on tcl") || terminal.startsWith("androidtvontcl")
                || terminal.startsWith("android_tv_on_tcl")) {
            filterTerminal = "android tv on tcl";
        }
        if (terminal.startsWith("android tv on haier")) {
            filterTerminal = "android tv on haier";
        }
        if (terminal.startsWith("full aosp on tcl")) {
            filterTerminal = "full aosp on tcl";
        }
        if (terminal.startsWith("kktv")) {
            filterTerminal = "kktv";
        }
        return filterTerminal;
    }
}