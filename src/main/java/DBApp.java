import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;

import org.javatuples.Pair;
import org.javatuples.Tuple;

import java.util.*;



public class DBApp  implements DBAppInterface{

	public void init( ) {
		 try (PrintWriter writer = new PrintWriter(new File("metadata.csv"))) {

		    } catch (FileNotFoundException e) {
		      System.out.println(e.getMessage());
		    }
		
	}
	
	// this does whatever initialization you would like
	// or leave it empty if there is no code you want to
	// execute at application startup 
	// following method creates one table only
	// strClusteringKeyColumn is the name of the column that will be the primary
	// key and the clustering column as well. The data type of that column will
	// be passed in htblColNameType
	// htblColNameValue will have the column name as key and the data
	// type as value
	// htblColNameMin and htblColNameMax for passing minimum and maximum values
	// for data in the column. Key is the name of the column
	public void createTable(String strTableName,
	String strClusteringKeyColumn,
	Hashtable<String,String> htblColNameType,
	Hashtable<String,String> htblColNameMin,
	Hashtable<String,String> htblColNameMax )
	throws DBAppException{
		try {
			this.appendCsv(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	
	}
public void appendCsv(String name,
		String cluster,Hashtable<String,String> htblColNameType,
		Hashtable<String,String> htblColNameMin,
		Hashtable<String,String> htblColNameMax) throws IOException,DBAppException {
	
FileWriter pw =  new FileWriter("metadata.csv",true);
Enumeration<String> type = htblColNameType.keys();

while(type.hasMoreElements()) {
	Boolean clus = false;
	StringBuilder builder = new StringBuilder();
	String col = type.nextElement();
	
	String typ= htblColNameType.get(col);
	String min =htblColNameMin.get(col);
	String max = htblColNameMax.get(col);
	
	if(col.equals(cluster))
		clus = true;
	
	
	builder.append(name+","+col+","+typ+","+clus.toString()+","+"false,"+min+","+max+"\n");
	System.out.println(clus.toString());
	pw.write(builder.toString());
	
	
	
	
	
}
pw.close();
System.out.print("done!");











//String columnNamesList = "Id,name";
//builder.append(columnNamesList +"\n");
//builder.append("1"+",");
//builder.append("opppssoeeee");

	
	
}


	

@Override
public void createIndex(String tableName, String[] columnNames) throws DBAppException {
	// TODO Auto-generated method stub
	
}
@Override
public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
	Table t1;
    

	
}
@Override
public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
		throws DBAppException {
	// TODO Auto-generated method stub
	
}
@Override
public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
	// TODO Auto-generated method stub
	
}
@Override
public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
	// TODO Auto-generated method stub
	return null;
}

public static int binarysearchint(Page v1, int key){
	int low = 0;
        int high = v1.size()-1;
		int mid =  high/2;
        while (low <= high) {
            
            int midVal = (int)(v1.get(mid).getValue(0));

            if (midVal<key)
                low = mid + 1;
            else if (midVal >key)
                high = mid - 1;
            else
                return mid; // key found
			mid = (low + high) /2;
        }
        return - 1;  // key not found
}
public static int binarysearchstring(Page v1, String key){
	int low = 0;
        int high = v1.size()-1;
		int mid = (low + high) /2;
        while (low <= high) {
            
            String midVal = (String)(v1.get(mid).getValue(0));
			int comp=midVal.compareTo(key);

            if (comp<0)
                low = mid + 1;
            else if (comp>0)
                high = mid - 1;
            else
                return mid; // key found
			mid = (low + high) /2;
        }
        return - 1;  // key not found
}
public static int binarysearchdate(Page v1, Date key){
	int low = 0;
        int high = v1.size()-1;
		int mid = (low + high) /2;
        while (low <= high) {
            
			Date midVal = (Date)(v1.get(mid).getValue(0));
			int comp=midVal.compareTo(key);

            if (comp<0)
                low = mid + 1;
            else if (comp>0)
                high = mid - 1;
            else
                return mid; // key found
			mid = (low + high) /2;
        }
        return - 1;  // key not found
}
public static int binarysearchdouble(Page v1, Double key){
	int low = 0;
        int high = v1.size()-1;
		int mid = (low + high) /2;
        while (low <= high) {
            
            Double midVal = (Double)(v1.get(mid).getValue(0));
			BigDecimal comp=BigDecimal.valueOf(midVal);
			BigDecimal key1=BigDecimal.valueOf(key);
			int cond=comp.compareTo(key1);

            if (cond<0)
                low = mid + 1;
            else if (cond==0)
			        return mid; // key found
            else
			high = mid - 1;
                
			mid = (low + high) /2;
        }
        return - 1;  // key not found
}

public static int binarysearchtableint(String t1,int key){
	int tablesize=Table.deserialT(t1);
	int high=(tablesize-1);
	int mid =(high)/2;
	int first=0;
	String g;
	while(first<=high){
		g="test"+(mid);
		if(key>((int)(Page.deserialP(g).firstElement()).getValue(0)) && key>((int)(Page.deserialP(g).lastElement().getValue(0))))
		{
		first=mid+1;
		}
		else if(((int)(Page.deserialP(g).firstElement()).getValue(0))<=key && key<=((int)(Page.deserialP(g).lastElement().getValue(0)))){
		break;
		}
		else{ 
		high=mid-1;
		}
		
		mid=(first+high)/2;
	}
	if(first>high){
	return -1;
	}
	else {
	 return mid;
	}	
}

public static int binarysearchtablestring(String t1,String key){
	int tablesize=Table.deserialT(t1);
	int high=(tablesize-1);
	int mid =(high)/2;
	int first=0;
	String g;
	while(first<=high){
		g="test"+(mid);
		int firstele=key.compareTo((String)(Page.deserialP(g).firstElement()).getValue(0));
		int lastele= key.compareTo((String)(Page.deserialP(g).lastElement()).getValue(0));
		if(firstele>0 && lastele>0){
		first=mid+1;
		}
		else if(firstele>=0 && lastele<=0){
		break;
		}
		else{ 
		high=mid-1;
		}
		
		mid=(first+high)/2;
	}
	if(first>high){
	return -1;
	}
	else {
	 return mid;
	}	
}
public static int binarysearchtabledouble(String t1,Double key){
	int tablesize=Table.deserialT(t1);
	int high=(tablesize-1);
	int mid =(high)/2;
	int first=0;
	String g;
	BigDecimal key1=BigDecimal.valueOf(key);
	while(first<=high){
		g="test"+(mid);
		BigDecimal firstele=BigDecimal.valueOf((Double)(Page.deserialP(g).firstElement()).getValue(0));
		BigDecimal lastele=BigDecimal.valueOf((Double)(Page.deserialP(g).lastElement()).getValue(0));
		int comp1=key1.compareTo(firstele);
		int comp2= key1.compareTo(lastele);
		if(comp1>0 && comp2>0){
		first=mid+1;
		}
		else if(comp1>=0 && comp2<=0){
		break;
		}
		else{ 
		high=mid-1;
		}
		
		mid=(first+high)/2;
	}
	if(first>high){
	return -1;
	}
	else {
	 return mid;
	}	
}
public static int binarysearchtabledate(String t1,Date key){
	int tablesize=Table.deserialT(t1);
	int high=(tablesize-1);
	int mid =(high)/2;
	int first=0;
	String g;
	while(first<=high){
		g="test"+(mid);
		int firstele=key.compareTo((Date)(Page.deserialP(g).firstElement()).getValue(0));
		int lastele= key.compareTo((Date)(Page.deserialP(g).lastElement()).getValue(0));
		if(firstele>0 && lastele>0){
		first=mid+1;
		}
		else if(firstele>=0 && lastele<=0){
		break;
		}
		else{ 
		high=mid-1;
		}
		
		mid=(first+high)/2;
	}
	if(first>high){
	return -1;
	}
	else {
	 return mid;
	}	
}
public static int[] searchtable(String t, Object key){
	int[] res=new int[2];
	if (key.getClass() == Integer.class) {
		int b1=binarysearchtableint(t, (int)key);
		if(b1>=0){
		Page p1=Page.deserialP("test"+b1);
		int b2=binarysearchint(p1,(int) key);
		res[0]=b1;
		res[1]=b2;
		return res ;
	} }
	else if (key.getClass() == String.class) {
		int b1=binarysearchtablestring(t, (String)key);
		if(b1>=0){
		Page p1=Page.deserialP("test"+b1);
		int b2=binarysearchstring(p1,(String) key);
		res[0]=b1;
		res[1]=b2;
		return res ;
	}
	}
	else if (key.getClass() == Double.class) {
		int b1=binarysearchtabledouble(t, (double)key);
		if(b1>=0){
		Page p1=Page.deserialP("test"+b1);
		int b2=binarysearchdouble(p1,(double) key);
		res[0]=b1;
		res[1]=b2;
		return res ;
	}
	}
	else if(key.getClass() == Date.class){
		int b1=binarysearchtabledate(t, (Date)key);
		if(b1>=0){
		Page p1=Page.deserialP("test"+b1);
		int b2=binarysearchdate(p1,(Date) key);
		res[0]=b1;
		res[1]=b2;
		return res ;
	}
	}
	res[0]=-1;
	res[1]=-1;
	return res;

}

@SuppressWarnings("unchecked")
public static void main(String[]args) throws IOException {

	 DBApp db = new DBApp();
	 db.init();
	 Hashtable htblColNameType = new Hashtable( );
	 htblColNameType.put("id", "java.lang.Integer"); 
	 htblColNameType.put("name", "java.lang.String");
	 htblColNameType.put("gpa", "java.lang.double");
	 
	 Hashtable htblColNameMin = new Hashtable();
	 htblColNameMin.put("id", "0");
	 htblColNameMin.put("name", " ");
	 htblColNameMin.put("gpa", "0");
	 
	 Hashtable htblColNameMax = new Hashtable();
	 htblColNameMax.put("id", "213981");
	 htblColNameMax.put("name", "ZZZZZZZZZZ");
	 htblColNameMax.put("gpa", "5");
	 
	 try {
		db.createTable("Test","id", htblColNameType, htblColNameMin, htblColNameMax);
	} catch (DBAppException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	 
	Page test0 = new Page();  
	//Adding elements to a vector  
	Pair<Integer, String> pair1 = new Pair<Integer, String>(Integer.valueOf(1), "Geeks");
	Pair<Integer, String> pair2 = new Pair<Integer, String>(Integer.valueOf(2), "Geeks");
	Pair<Integer, String> pair3 = new Pair<Integer, String>(Integer.valueOf(3), "Geeks");
	Pair<Integer, String> pair4 = new Pair<Integer, String>(Integer.valueOf(4), "Geeks");
	test0.add(pair1);
	test0.add(pair2);
	test0.add(pair3);
	test0.add(pair4);
	

	Page test1 = new Page();  
	//Adding elements to a vector  
	Pair<Integer, String> pair5 = new Pair<Integer, String>(Integer.valueOf(5), "Geeks");
	Pair<Integer, String> pair6 = new Pair<Integer, String>(Integer.valueOf(6), "Geeks");
	Pair<Integer, String> pair7 = new Pair<Integer, String>(Integer.valueOf(7), "Geeks");
	Pair<Integer, String> pair8 = new Pair<Integer, String>(Integer.valueOf(8), "Geeks");
	test1.add(pair5);
	test1.add(pair6);
	test1.add(pair7);
	test1.add(pair8);
	

	Page test2 = new Page();  
	//Adding elements to a vector  
	Pair<Integer, String> pair9 = new Pair<Integer, String>(Integer.valueOf(9), "Geeks");
	Pair<Integer, String> pair10 = new Pair<Integer, String>(Integer.valueOf(10), "Geeks");
	Pair<Integer, String> pair11 = new Pair<Integer, String>(Integer.valueOf(11), "Geeks");
	Pair<Integer, String> pair12= new Pair<Integer, String>(Integer.valueOf(12), "Geeks");
	test2.add(pair9);
	test2.add(pair10);
	test2.add(pair11);
	test2.add(pair12);

	Page test3 = new Page();  
	//Adding elements to a vector  
	Pair<Integer, String> pair13 = new Pair<Integer, String>(Integer.valueOf(13), "Geeks");
	Pair<Integer, String> pair14= new Pair<Integer, String>(Integer.valueOf(14), "Geeks");
	Pair<Integer, String> pair15= new Pair<Integer, String>(Integer.valueOf(15), "Geeks");
	Pair<Integer, String> pair16= new Pair<Integer, String>(Integer.valueOf(16), "Geeks");
	test3.add(pair13);
	test3.add(pair14);
	test3.add(pair15);
	test3.add(pair16);

	Table t1 = new Table("tests");
	t1.add(test0);
	t1.add(test1);
	t1.add(test2);
	t1.add(test3);
	test0.serialP("test0");
	test1.serialP("test1");
	test2.serialP("test2");
	test3.serialP("test3");
	t1.serialT();
System.out.println(Arrays.toString(searchtable("tests",12)));

	

	/*Page test1 = new Page();  
	//Adding elements to a vector  
	Object e=14;
	Object f=15.0;
	Object g=16.0;
	Object h=17.0;
	test1.add(e);
	test1.add(f);
	test1.add(g);
	test1.add(h);

	Page test2 = new Page();  
	//Adding elements to a vector  
	Object i=18.0;
	Object j=19.0;
	Object k=20.0;
	Object l=21.0;
	test2.add(i);
	test2.add(j);
	test2.add(k);
	test2.add(l);
	
	Page test3 = new Page();  
	//Adding elements to a vector  
	Object m=22.0;
	Object n=23.0;
	Object o=24.0;
	Object p=25.0;
	test3.add(m);
	test3.add(n);
	test3.add(o);
	test3.add(p);
	
	Table t1 = new Table("tests");
	t1.add(test0);
	t1.add(test1);
	t1.add(test2);
	t1.add(test3);
	test0.serialP("test0");
	test1.serialP("test1");
	test2.serialP("test2");
	test3.serialP("test3");
	t1.serialT();
System.out.println(binarysearchtabledouble("tests",25.0));*/

	
	
	

}
}


