import java.io.*;
import java.util.Properties;
import java.util.Vector;

public class Bucket extends Vector<Object> implements Serializable {

    public Bucket() {

    }
    public boolean isFull(){
        boolean res=false;
        Properties prop = new Properties();
        String fileName = "src/main/resources/DBApp.config";
        InputStream is = null;
        try {
            is = new FileInputStream(fileName);
        } catch (FileNotFoundException ignored) {

        }
        try {
            prop.load(is);
            int max = Integer.parseInt(prop.getProperty("MaximumKeysCountinIndexBucket"));
            if (this.size()==max){
                res= true;
            }
            else
                res= false;
        }
        catch (IOException ex) {


        }

        return res;
    }

    }

