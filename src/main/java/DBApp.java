import java.io.*;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


public class DBApp  implements DBAppInterface{






	public void init( ) {

		try {
			BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			if (br.readLine() == null) {
				System.out.println("No errors, and file empty");
				FileWriter pw =  new FileWriter("src/main/resources/metadata.csv",true);
				pw.write("Table Name, Column Name, Column Type, ClusteringKey, Indexed, min, max \n");
				pw.close();
			}

			Path path = Paths.get("src/main/resources/data"); //create data directory
			Files.createDirectories(path);

		} catch (IOException e) {

			System.err.println("Failed to create directory!" + e.getMessage());

		}
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



	public static boolean tableExists(String name){
		boolean res= false;


		String line = "";
		String splitBy = ",";
		try
		{
			BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			while ((line = br.readLine()) != null)   //returns a Boolean value
			{
				String[] row = line.split(splitBy);    // use comma as separator
				String line0 = row[0];


				if (line0.equalsIgnoreCase(name)){
					res=true;
					break;
				}

			}

		}
		catch (IOException e)
		{
			e.printStackTrace();

		}


		return res;
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
		Comparator<ArrayList<Object>> comparator = new Com();
		ArrayList<Object> row = new ArrayList <>();
		String cluster= Table.returnCluster(tableName);
		String columns []=Table.returnColumns(tableName);


		if(!(colNameValue.containsKey(cluster))){ // no cluster key was inserted
			throw  new DBAppException();
		}

		Object valu= colNameValue.get(cluster); //cluster key value
		if(valu==null){ //check if cluster value = null
			throw new DBAppException();
		}

		int[]index = searchtable(tableName,valu);

		if((index[1]!=-1)){ //check if cluster value = null or cluster value exists already in table
			throw new DBAppException();
		}

			for (int i = 0; i < columns.length; i++) {
				String col = columns[i];
				Object x = colNameValue.get(col);
				int ty = getType(tableName, col); //int 0 Double 1 String 2 Date 3
				if (x == null) {
					row.add(x);
				} else if (((x instanceof Integer) && ty != 0) || ((x instanceof Double) && ty != 1) || ((x instanceof String) && ty != 2) || ((x instanceof Date) && ty != 3)) {
					throw new DBAppException();
				} else {
					boolean check = checkMinMax(tableName, col, x);
					if (!check)
						throw new DBAppException();
					row.add(x);
				}
			}// done with the tuple


				//check if table is empty or not
				if(Table.deserialT(tableName)==0){
					Page p = new Page();
					p.add(row);
					p.serialP(tableName+0);
					Table.insertInto(tableName);
				}

				else {
					if (index[0] != -1) {
						Page p = Page.deserialP(tableName + index[0]);
						boolean res = p.isFull();
						if (!res) {
							p.add(row);
							Collections.sort(p, comparator);
							//p.serialP();
						}
						//check if full first
						//sort needed here
						p.serialP(tableName + index[0]);

					}
				}















	}


	@Override
	public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
		throws DBAppException {
			Table.deserialT(tableName);
			String[] columns=Table.returnColumns(tableName);
           int type= getType(tableName, columns[0]);
		   Object clusterkey=-1;
		   if (type==0){
		   clusterkey=Integer.parseInt(clusteringKeyValue);
		   }
		   else if(type==1){
			clusterkey=Double.parseDouble(clusteringKeyValue);
		   }
		   else if(type==3){
			DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
			try {
				clusterkey = format.parse(clusteringKeyValue);
			} catch (ParseException e) {
				e.printStackTrace();
			}
		   }

		   else{
			clusterkey=clusteringKeyValue;
		   }
			int[] pos=searchtable(tableName,clusterkey);
			if(pos[0]==-1 || pos[1]==-1){
				throw new DBAppException();
			}
			Enumeration<String> keys = columnNameValue.keys();
			String s=tableName+pos[0];
			Page p=Page.deserialP(s);
			while(keys.hasMoreElements()){
			String key=keys.nextElement();
			int j=coloumnnum(key, tableName);
			if(j==-1){
				throw new DBAppException();
			}
            Object Change = columnNameValue.get(key);
			

			
			p.get(pos[1]).set(j, Change);
			
			

	}
	p.serialP(s);
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

	public static int coloumnnum(String coloumn, String t1){
    String[] array=Table.returnColumns(t1);
	for(int i=0;i<array.length;i++){
    if (array[i].equals(coloumn))
		return i;
	
	}
    return -1;
}








	public static void main(String[]args) throws DBAppException  {
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



			Hashtable htblColNameValue = new Hashtable();
			htblColNameValue.put("id", new Integer(240));
			htblColNameValue.put("name", new String("aaaa"));
			htblColNameValue.put("gpa", new Double(3));


	}

}