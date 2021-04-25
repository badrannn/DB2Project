import java.io.*;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.*;



public class DBApp  implements DBAppInterface{

	public void init( ) {

	}
	

	public void createTable(String strTableName,
	String strClusteringKeyColumn,
	Hashtable<String,String> htblColNameType,
	Hashtable<String,String> htblColNameMin,
	Hashtable<String,String> htblColNameMax )
	throws DBAppException{
		try {
			this.appendCsv(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax);
			Table t = new Table(strTableName);

			t.cluster = strClusteringKeyColumn;

			t.columns=  new String [htblColNameType.size()];
			t.columns[0]=strClusteringKeyColumn;

			htblColNameType.remove(strClusteringKeyColumn);
			Enumeration<String> keys = htblColNameType.keys();
			for(int i = 1; i<t.columns.length;i++){
				t.columns[i]=keys.nextElement();
			}

			t.serialT();

		} catch (IOException e) {

			e.printStackTrace();
		}
		
	
	}
	public void appendCsv(String name,
		String cluster,Hashtable<String,String> htblColNameType,
		Hashtable<String,String> htblColNameMin,
		Hashtable<String,String> htblColNameMax) throws IOException {
	
		FileWriter pw =  new FileWriter("src/main/resources/metadata.csv",true);
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
	}







	public static boolean checkMinMax(String table, String col, Object key) throws DBAppException {
		int type= getType(table,col); //int 0 Double 1 String 2 Date 3
		String min="";
		String max="";

		String s ="";
		String line = "";
		String splitBy = ",";
		try {
			BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			while ((line = br.readLine()) != null)   //returns a Boolean value
			{
				String[] row = line.split(splitBy);    // use comma as separator
				String line0 = row[0];
				String line1 = row[1];

				if (line0.equalsIgnoreCase(table) && line1.equalsIgnoreCase(col)) {

					min = row[5];
					max=row[6];
					break;
				}

			}
			if (type==0){
				int obj = (int) key;
				try{
					int mi = Integer.parseInt(min);
					int ma = Integer.parseInt(max);
					if(obj<=ma && obj>=mi)
						return true;
					else return false;
				}
				catch (Exception e) {
					throw new DBAppException();

				}
			}
			else if (type==1){
				Double obj = (Double) key;
				BigDecimal obj1=BigDecimal.valueOf(obj);
				try{
					Double mi = Double.parseDouble(min);
					Double ma = Double.parseDouble(max);
					BigDecimal mi1=BigDecimal.valueOf(mi);
					BigDecimal ma1=BigDecimal.valueOf(ma);
					int comp1=obj1.compareTo(mi1);
					int comp2= obj1.compareTo(ma1);


					if(comp1>=0 && comp2<=0)
						return true;
					else return false;
				}
				catch (Exception e) {
					throw new DBAppException();

				}
			}
			else if (type==3){


				try{
					DateFormat format = new SimpleDateFormat("yyyy-MM-dd");

					Date mi = format.parse(min);
					Date ma = format.parse(max);
					Date obj = (Date)key;

					int comp1=obj.compareTo(mi);
					int comp2=obj.compareTo(ma);

					if(comp1>=0 && comp2<=0)
						return true;
					else return false;
				}
				catch (Exception e) {
					throw new DBAppException();

				}
			}
			else {
				String obj = (String) key;
				int comp1=obj.compareToIgnoreCase(min);
				int comp2=obj.compareToIgnoreCase(max);

				if(comp1>=0 && comp2<=0)
					return true;
				else return false;
			}





		}
		catch (IOException e)
		{
			e.printStackTrace();
			return false;
		}

	}


	public static int getType(String name,String col){ //int 0 Double 1 String 2 Date 3
		String s ="";
		String line = "";
		String splitBy = ",";
		try
		{
			BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			while ((line = br.readLine()) != null)   //returns a Boolean value
			{
				String[] row = line.split(splitBy);    // use comma as separator
				String line0 = row[0];
				String line1= row[1];

				if (line0.equalsIgnoreCase(name) && line1.equalsIgnoreCase(col)){

					s=row[2];
					break;
				}

			}
			if (s.equalsIgnoreCase("java.lang.Integer"))
				return 0;
			else if(s.equalsIgnoreCase("java.lang.Double"))
				return 1;
			else if (s.equalsIgnoreCase("java.lang.String"))
				return 2;
			else return 3;




		}
		catch (IOException e)
		{
			e.printStackTrace();
			return -1;
		}
	}


	

	@Override
	public void createIndex(String tableName, String[] columnNames) throws DBAppException {
	// TODO Auto-generated method stub
	
	}
	@Override
	public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
		ArrayList<Object> row = new ArrayList <>();
		String cluster= Table.returnCluster(tableName);
		if((colNameValue.containsKey(cluster))==false){ // no cluster key was inserted
			throw  new DBAppException();
		}
		Object valu= colNameValue.get(cluster);
		int[]index = searchtable(tableName,valu);
		if((index[1]!=-1) || valu==null){ //check if cluster value = null or cluster value exists already in table
			throw new DBAppException();
		}
		String columns []=Table.returnColumns(tableName);
		for (int i = 0; i < columns.length; i++) {
			String col= columns[i];
			Object x = colNameValue.get(col);
			int ty = getType(tableName,col); //int 0 Double 1 String 2 Date 3
			if (x==null){
				row.add(x);
			}
			else if(((x instanceof Integer)&&ty!=0) || ((x instanceof Double)&&ty!=1) || ((x instanceof String)&&ty!=2) || ((x instanceof Date)&&ty!=3)) {
				throw  new DBAppException();
			}
			else {
				boolean check = checkMinMax(tableName,col,x);
				if(check==false)
					throw  new DBAppException();
				row.add(x);
			}
			if (index[0]!=-1) {
				Page p = Page.deserialP(tableName + index[0]);
//				boolean res =p.isFull();
				p.add(row);
				//check if full first
				//sort needed here
				p.serialP(tableName + index[0]);

			}
		}











	}
	@Override
	public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
		throws DBAppException {


	}
	@Override
	public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {

	}
	@Override
	public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {

		return null;
	}

	public static int binarysearchint(Page v1, int key){
		int low = 0;
        int high = v1.size()-1;
		int mid =  high/2;
        while (low <= high) {
            
            int midVal = (int)(v1.get(mid).get(0));

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
            
            String midVal = (String)(v1.get(mid).get(0));
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
            
			Date midVal = (Date)(v1.get(mid).get(0));
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
            
            Double midVal = (Double)(v1.get(mid).get(0));
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
			g=t1+(mid);
			if(key>((int)(Page.deserialP(g).firstElement()).get(0)) && key>((int)(Page.deserialP(g).lastElement().get(0))))
			{
				first=mid+1;
			}
		else if(((int)(Page.deserialP(g).firstElement()).get(0))<=key && key<=((int)(Page.deserialP(g).lastElement().get(0)))){
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
		g=t1+(mid);
		int firstele=key.compareTo((String)(Page.deserialP(g).firstElement()).get(0));
		int lastele= key.compareTo((String)(Page.deserialP(g).lastElement()).get(0));
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
		g=t1+(mid);
		BigDecimal firstele=BigDecimal.valueOf((Double)(Page.deserialP(g).firstElement()).get(0));
		BigDecimal lastele=BigDecimal.valueOf((Double)(Page.deserialP(g).lastElement()).get(0));
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
		g=t1+(mid);
		int firstele=key.compareTo((Date)(Page.deserialP(g).firstElement()).get(0));
		int lastele= key.compareTo((Date)(Page.deserialP(g).lastElement()).get(0));
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
public static int[] searchtable(String t, Object key){  //[page,index inside specified page]
	int[] res=new int[2];
	if (key instanceof Integer) {
		int b1=binarysearchtableint(t, (int)key);
		if(b1>=0){
		Page p1=Page.deserialP(t+b1);
		int b2=binarysearchint(p1,(int) key);
		res[0]=b1;
		res[1]=b2;
		return res ;
	} }
	else if (key instanceof Double) {
		int b1=binarysearchtabledouble(t, (double)key);
		if(b1>=0){
		Page p1=Page.deserialP(t+b1);
		int b2=binarysearchdouble(p1,(double) key);
		res[0]=b1;
		res[1]=b2;
		return res ;
	}
	}
	else if(key instanceof Date){
		int b1=binarysearchtabledate(t, (Date)key);
		if(b1>=0){
		Page p1=Page.deserialP(t+b1);
		int b2=binarysearchdate(p1,(Date) key);
		res[0]=b1;
		res[1]=b2;
		return res ;
	}
	}
	else if (key instanceof String) {
		int b1=binarysearchtablestring(t, (String)key);
		if(b1>=0){
			Page p1=Page.deserialP(t+b1);
			int b2=binarysearchstring(p1,(String) key);
			res[0]=b1;
			res[1]=b2;
			return res ;
		}
	}
	res[0]=-1;
	res[1]=-1;
	return res;

}







	public static void main(String[]args) throws DBAppException, ParseException {
		DBApp db = new DBApp();
		db.init();
		Hashtable htblColNameType = new Hashtable( );
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.double");
		htblColNameType.put("date", "java.util.Date");


		Hashtable htblColNameMin = new Hashtable();
		htblColNameMin.put("id", "0");
		htblColNameMin.put("name", " ");
		htblColNameMin.put("gpa", "0");
		htblColNameMin.put("date", "2000-01-1");

		Hashtable htblColNameMax = new Hashtable();
		htblColNameMax.put("id", "213981");
		htblColNameMax.put("name", "ZZZZZZZZZZ");
		htblColNameMax.put("gpa", "5");
		htblColNameMax.put("date", "2021-04-15");


		try {
			db.createTable("Test","id", htblColNameType, htblColNameMin, htblColNameMax);

		} catch (DBAppException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Page test0 = new Page();
		//Adding elements to a vector
		ArrayList<Object> a4 = new ArrayList<>();
		ArrayList<Object> a5 = new ArrayList<>();

		a4.add(5.4);
		a5.add(1.12);


		test0.add(a5);
		test0.add(a4);
		Table t1 = new Table("test");
		t1.add(test0);
		t1.serialP();
		t1.serialT();

		Page p = Page.deserialP("test0");
		boolean b = p.isFull();


		//System.out.println(b);


	}
}