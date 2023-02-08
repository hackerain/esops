package com.example;

import java.util.HashSet;
import java.util.Set;
import java.io.*;

public class Diff {

    public static Set<String> read(String file) {
        Set<String> set = new HashSet<>();
        try {
            BufferedReader in = new BufferedReader(new FileReader(file));
            String str;
            while ((str = in.readLine()) != null) {
                set.add(str.strip());
            }
            in.close();
        } catch (IOException e) {
            System.out.println("Fail to read: " + e);
        }
        return set;
    }

    public static void write(Set<String> set, String file) {
        try {
            File f = new File(file);
            if (f.exists()) {
                f.delete();
            }
            BufferedWriter out = new BufferedWriter(new FileWriter(f.getName(), true));
            for (String s : set) {
                out.write(s+"\n");
            }
            out.close();
        } catch (IOException e) {
            System.out.println("Fail to write: " + e);
        }
   }

    public static void main(String[] args) {

        String index1 = "index1.txt";
        String index2 = "index2.txt";

        Set<String> result = new HashSet<>();

        System.out.println("reading index1");
        Set<String> set1 = read(index1);
        System.out.println("reading index1 done");

        System.out.println("reading index2");
        Set<String> set2 = read(index2);
        System.out.println("reading index2 done");

        // 交集
        System.out.println("caculating intersection");
        result.clear();
        result.addAll(set1);
        result.retainAll(set2);
        System.out.println("caculating intersection done");

        System.out.println("writing intersection");
        write(result, "index_intersection.txt");
        System.out.println("writing intersection done");

        // 差集
        System.out.println("caculating difference");
        result.clear();
        result.addAll(set1);
        result.removeAll(set2);
        System.out.println("caculating difference done");

        System.out.println("writing difference");
        write(result, "index_diff.txt");
        System.out.println("writing difference done");

        // 并集
        System.out.println("caculating union");
        result.clear();
        result.addAll(set1);
        result.addAll(set2);
        System.out.println("caculating union done");

        System.out.println("writing union");
        write(result, "index_union.txt");
        System.out.println("writing union done");
    }
}
