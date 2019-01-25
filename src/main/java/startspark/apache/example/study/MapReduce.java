package startspark.apache.example.study;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MapReduce {

    public static void main(String args[]){

        //list of 6 entries
        List<String> items = new ArrayList();
        items.add("bob");
        items.add("susan");
        items.add("julie");
        items.add("bob");
        items.add("bob");
        items.add("julie");

        //map reduces list of six to 3 entries
        Map<String, Integer> map = new HashMap();
        for(String s: items){
            if(map.containsKey(s)){
                Integer i = map.get(s);
                map.put(s, i+1);
            }else{
                map.put(s, 1);
            }
        }

        for (Map.Entry<String, Integer> entry : map.entrySet())
        {
            System.out.println(entry.getKey() + "/" + entry.getValue());
        }

        /*
        julie/2
        bob/3
        susan/1
         */

    }
}
