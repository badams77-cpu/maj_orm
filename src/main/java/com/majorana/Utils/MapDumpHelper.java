package com.majorana.Utils;

import java.util.Comparator;
import java.util.Map;
import java.util.stream.Collectors;

public class MapDumpHelper {


    public static String dump(Map<String , String> map, String COL_SEP, String ROW_SEP){
        StringBuffer buf = new StringBuffer();
        buf.append("Map size= "+map.size()+ROW_SEP);
        buf.append("Keys = "+map.keySet().stream().sorted().collect(Collectors.joining(COL_SEP))+ROW_SEP);
        buf.append(map.entrySet().stream().sorted(
                Comparator.comparing(Map.Entry::getKey) ).map(
                        e -> e.getKey()+" = " +e.getValue() ).collect(Collectors.joining(ROW_SEP)));
  //      for( String key : map.keySet()){
  //                buf.append("   "+key+"="+map.get(key)+LINE_SEP);
  //      }
        return buf.toString();
    }

}
