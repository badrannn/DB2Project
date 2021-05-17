import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;

public class Com implements Comparator<ArrayList<Object>> {
    @Override
    public int compare(ArrayList <Object> o1, ArrayList <Object> o2) {
        int res=-1;
        Object key1 = o1.get(0);
        Object key2= o2.get(0);

        if(key1 instanceof Integer){
           int k = (int) key1;
           int k1 = (int) key2;
           if(k>k1){
               return 1;
           }
           else {
               return -1;
           }
        }
        else if(key1 instanceof Double){
            Double k = (Double) key1;
            Double k1 = (Double)  key2;
            BigDecimal kk= BigDecimal.valueOf(k);
            BigDecimal kk2= BigDecimal.valueOf(k1);

            if (kk.compareTo(kk2)>0){
                return 1;
            }
            else
                return -1;
        }
        else if(key1 instanceof String){
            String k = (String) key1;
            String k1 = (String)  key2;
            if (k.compareTo(k1)>0){
                return 1;
            }
            else return -1;
        }
        else {
            Date k = (Date) key1;
            Date k1 =(Date) key2;

            if (k.compareTo(k1)>0){
                return 1;
            }
            else return -1;
        }
    }
}
