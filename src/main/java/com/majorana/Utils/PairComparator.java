package com.majorana.Utils;

import org.apache.commons.lang3.tuple.Pair;

import java.util.Comparator;

public class PairComparator implements Comparator<Pair<Integer,Integer>> {

        @Override
        public int compare(Pair<Integer,Integer> o1, Pair<Integer,Integer> o2) {
            if (o1==null && o2!=null){ return -1; }
            if (o1!=null && o2==null){ return 1; }
            if (o1==null && o2==null){ return 0; }
            if (o1.getLeft()<o2.getLeft()){ return -1; }
            if (o1.getLeft()>o2.getLeft()){ return 1; }
            if (o1.getRight()<o2.getRight()){ return -1; }
            if (o1.getRight()>o2.getRight()){ return 1; }
            return 0;
        }

        @Override
        public Comparator<Pair<Integer,Integer>> reversed() {
            return Comparator.super.reversed();
        }







}
