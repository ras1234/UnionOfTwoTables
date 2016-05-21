import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Random;
import java.util.Collections;
import java.util.Date;
import java.util.List;


public class Generator {
	public static final int MAX=10000;
	public static final int MIN=1; 
	private HashMap<String,String> MetadataColumnDatatypes=new HashMap<String,String>();
	private int RecordSize=0;
	private String[] metadata_datatypes;
	
    public Generator(int NumberOfAttributes){
    	int ColumnNumber;
    	String ColumnDatatype;
    	//metadata_datatypes = new String[] {"int","char(13)","date"};
    	//metadata_datatypes=new String[] {"int","char(13)"};
    	metadata_datatypes=new String[] {"char(13)"};
    	Random randnumber=new Random();
    	BufferedWriter metadata_Wr = null;
    	try {
			metadata_Wr=new BufferedWriter(new FileWriter(new File("metadata.txt")));
		} catch (IOException e) {
			e.printStackTrace();
		}
    	for(int i=0;i<NumberOfAttributes;i++)
    	{
    		//ColumnNumber=randnumber.nextInt(3);
    		//ColumnNumber=randnumber.nextInt(2);
    		ColumnNumber=randnumber.nextInt(1);
    		ColumnDatatype=metadata_datatypes[ColumnNumber];
    		RecordSize+=1;
    		switch(ColumnDatatype)
    		{
//    			case "int":
//    				RecordSize+=6;
//    				break;
    			case "char(13)":
    				RecordSize+=10;
    				break;
//    			case "date":
//    				RecordSize+=10;
//    				break;
    		}
    		
    		MetadataColumnDatatypes.put("col"+String.valueOf(i+1),ColumnDatatype);
    		
			try{
				metadata_Wr.write("col" + String.valueOf(i + 1) + "," + ColumnDatatype);
				if(i + 1 != NumberOfAttributes)
					metadata_Wr.write("\n");
			}catch (IOException e) {
				e.printStackTrace();
			}
    	}
    	RecordSize += 1;
		
		try {
			metadata_Wr.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
  
    public double randomWithRange(double range){
       range = (MAX - MIN) + 1;     
       return (Math.random()*(double)range) + MIN;
    }
    
    public int randomint(Random randnumber){
    	return randnumber.nextInt(MAX-MIN)+MIN;
    }
    
    public static int randBetween(int start, int end) {
        return start + (int)Math.round(Math.random() * (end - start));
    }
    

    public void populatedata(String tablename,int NumberOfAttributes,double tablesize,int duplicates)
    {   
    	String datatype;
    	String alphabets="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890";
    	long tablesizeCounter=0;
    	long tupleCounter=0;
    	tablesize=tablesize*1024*1024;
    	FileWriter table_Wr=null;
    	try {
			table_Wr=new FileWriter(new File(tablename));
		} catch (IOException e) {
			e.printStackTrace();
		}
    	List <String> tupleslist = new ArrayList <String>();
    	ArrayList<Integer> list=new ArrayList<Integer>();
    	for(int i=0;i<100;i++){
    		list.add(i);
    	}
    	while(tablesizeCounter < tablesize)
    	{
    		StringBuffer tuple=new StringBuffer(RecordSize);
    		for(int i=0;i<NumberOfAttributes;i++)
    		{
    			datatype=MetadataColumnDatatypes.get("col" + String.valueOf(i+1));
    			switch(datatype)
    			{
//    				case "int":
//    					Random randnumber=new Random();
//    					Integer rand=randomint(randnumber);
//    					tuple.append(rand);
//    					break;
    				case "char(13)" :
						int charlen = alphabets.length();
						int j = 0;
						while (j < 13) {
							double index = Math.random() * (double)charlen;
						    tuple.append(alphabets.charAt((int)index));
						    ++j;
						 }
						break;
//    				case "date":
//    					 GregorianCalendar gc = new GregorianCalendar();
//    				     int year = randBetween(1900, 2014);
//    				     gc.set(gc.YEAR,year);
//    				     int dayOfYear = randBetween(1, gc.getActualMaximum(gc.DAY_OF_YEAR));
//    				     gc.set(gc.DAY_OF_YEAR, dayOfYear);
//    				     String date=gc.get(gc.DAY_OF_MONTH) + "/" + (gc.get(gc.MONTH) + 1) + "/" + gc.get(gc.YEAR);
//    				     tuple.append(date);
//    					break;
    			}
    			if((i+1)!=NumberOfAttributes)
    				tuple.append(',');
    		}
    		if(tupleCounter++==100)
    		{
    			Collections.shuffle(list);
    			for(int k=0;k<duplicates;k++)
    			{
    				tupleslist.add(tupleslist.get(list.get(k)));
    			}
    			
    			for(int k = 0; k < 100 + duplicates && tablesizeCounter < tablesize; k++) {
					try {
						table_Wr.write(tupleslist.get(k) + "\n");
					} catch (IOException e) {
						e.printStackTrace();
					}
					tablesizeCounter += RecordSize;
				}
    			tupleslist.clear();
				tupleCounter=0;
    		}
    		else
    		{
    			tupleslist.add(tuple.toString());
    			tuple.setLength(0);
    		}
    	}
    	try {
			table_Wr.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
    public int getRecordSize()
    {	
		return this.RecordSize;		
	}
	
}

