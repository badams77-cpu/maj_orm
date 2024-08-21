package com.majorana.Util;

import Majorana.Utils.MapDumpHelper;
import Majorana.Utils.MethodPrefixingLoggerFactory;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.runners.Suite;

import static org.junit.Assert.*;
import org.junit.runner.Runner;


@RunWith(JUnitPlatform.class)
public class MapDumpHelperTest {


    private static final Logger LOGGER = MethodPrefixingLoggerFactory.getLogger(MapDumpHelperTest.class);

    private static final String[][] ROWX2 =  { {"K1", "V1" }, {"K2","V2"}, {"password", "HIDE_PASS"} };

  //  private static final String ROW_SEPERATOR_PAT = "\\\\n"S

    private static  String ROW_SEPERATOR =  System.getProperty("line.separator");

    private static final String COL_SEPERATOR = ", ";

    private MapDumpHelper sut = new MapDumpHelper();

    private Map<String, String> empty = new HashMap<>();
    private Map<String, String> item1 = setMapDataFrom2DArray(ROWX2);



    @BeforeClass
    public static void setup() {
        ROW_SEPERATOR = System.getProperty("line.separator");
    }


    private static Map<String, String> setMapDataFrom2DArray( String[][] d){
        Map<String, String>  ret = new HashMap<>();
        for(int i= 0; i<d.length; i++){
            String[] row = d[i];
            if (row.length!=2){
                LOGGER.warn("Data row "+i+ "does not have two items, expect key and value");
            } else {
                ret.put(row[0],row[1]);
            }
        }
        return ret;
    }



    @Test
    public void MapDumnperHelperTest_A_Empty_Map(){
        String resEmptyMap = sut.dump( empty, COL_SEPERATOR , ROW_SEPERATOR);
        String lines[] = splitLines(resEmptyMap);
        assertEquals( "Map size= 0", lines[0]);
        assertEquals( "Keys = ", lines[1]);
//        assertEquals( "", lines[2]);
//        assertEquals( "Entries = ", lines[2]);
//        assertEquals( "", lines[3]);
    }

    @Test
    public void MapDumnperHelperTest_B_Two_Entry_Map_Map(){
        Map<String, String> myMap = setMapDataFrom2DArray(ROWX2);
        String resEmptyMap = sut.dump( myMap , COL_SEPERATOR, ROW_SEPERATOR );
        assertDataMatches( ROWX2 , resEmptyMap);
    }

    private static String getKeysString(String[][] array){
        String ret = Arrays.stream(array).filter(a->a.length>0).map(
                a-> a[0]
        ).collect(Collectors.joining(", "));
        return ret;

    }

    private static String[] splitLines(String in){
        String[] out = in.lines().collect(Collectors.toList()).toArray(new String[0]);
        return out;
    }

    private static void assertDataMatches( String[][] tdArray, String results ){
        String lines[] = splitLines(results);
        assertEquals( "Map size= "+tdArray.length, lines[0]);
        for(int i=1; i< tdArray.length; i++) {
            assertEquals("Keys = " + getKeysString(tdArray), lines[1]);
        }

        if (tdArray.length == 0) {
            assertEquals("", lines[2]);
        } else {
            for (int i = 0; i<tdArray.length; i++) {
                if (tdArray[i][0].equalsIgnoreCase("password")){
                    assertEquals(tdArray[i][0] + " = xxxx", lines[ 2 + i]);
                } else{
                    assertEquals(tdArray[i][0] + " = " + tdArray[i][1], lines[2 + i]);
                }
            }
        }
        int startEntries = 2+tdArray.length;
//        assertEquals( "Entries = ", startEntries );
//        assertEquals("", lines[2]);
        if (tdArray.length == 0) {
            assertEquals("", lines[startEntries+1]);
        } else {
       //     for (int i = 0; i < tdArray.length; i++) {
       //         assertEquals(tdArray[i][0]+" = " + tdArray[i][1], lines[startEntries+i]);
       //     }
        }
    }


}
