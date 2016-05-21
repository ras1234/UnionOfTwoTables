import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class UnionRelation {
	
	public static int RecordSize = 0;
	private static long currentFilePointerPosition = 0;
	public static int noOfRecordsInM = 0;
	public static HashMap<Character, HashSet <String> > buckets = new HashMap<Character, HashSet <String>>();
	public static List <String> bucketFileNames = new ArrayList<String>();
	
	public static void main(String[] args) {
		if(args.length!=5)
		{
			System.out.println("<table1> <table2> <Number_of_Attributes> <MemorySize> <type_of_Index> ");
			System.exit(0);
		}
		String table1=args[0];
		String table2=args[1];
		int MemorySize=Integer.parseInt(args[3]);
		int NumberOfAttributes=Integer.parseInt(args[2]);
		String Index_Type=args[4];
		int RecordSize=0;
		Generator generate_table1=new Generator(NumberOfAttributes);
		System.out.println("table1 is generating...");
		generate_table1.populatedata(table1,NumberOfAttributes,50,15);
		System.out.println("table1 generated");
		System.out.println("table2 is generating");
		generate_table1.populatedata(table2,NumberOfAttributes,50,15);
		System.out.println("table2 generated");	
		RecordSize=generate_table1.getRecordSize();
		
		System.out.print("Type of Index Selected...");
		if(Index_Type.compareToIgnoreCase("HASH") == 0){
			System.out.println("HASH");
			System.out.println("Union of Two Relations is generating...");
			try {
				HashedUnion(table1,table2,NumberOfAttributes,MemorySize,RecordSize);
			} catch (IOException e) {
				e.printStackTrace();
			}		
		} else {			
			System.out.print("B+TREE");
			System.out.println("Union of Two Relations is genererating...");
			UnionBtree(table1,table2,NumberOfAttributes,MemorySize,RecordSize);
			
		}
	}
	
	private static void HashedUnion(String table1,String table2,int NumberOfAttributes,int MemorySize,int RecordSize) throws IOException
	{
		noOfRecordsInM = 0;
//		HashMap<Character, HashSet <String> > buckets = new HashMap<Character, HashSet <String>>();
//		List <String> bucketFileNames = new ArrayList<String>();
		int noOfBuffers=36;
	
		//Access the first table1
		noOfRecordsInM = MemorySize / RecordSize;
		RandomAccessFile radomPointer = new RandomAccessFile(table1, "r");
		Accessrelation(radomPointer, noOfBuffers,MemorySize);
		radomPointer.close();
		//reset the seek pointer and Access the table2
		currentFilePointerPosition=0;
		radomPointer=new RandomAccessFile(table2,"r");
		Accessrelation(radomPointer,noOfBuffers,MemorySize);
		radomPointer.close();
		//	loop through the list of the bucket files and call merge
		String outputFilename="output.txt";
		int totalfilebuckets=bucketFileNames.size();
		for(int i=0;i<totalfilebuckets;i++)
			MergeFiles(outputFilename,bucketFileNames.get(i));
	}

private static void MergeFiles(String outputFilename, String bucketFilename) throws FileNotFoundException {

	System.out.println("i am creating a output file");
	HashSet<String> recordsbucket=new HashSet<String>();
	BufferedReader bucketfile=null;
	FileWriter outputWriter=null;
	bucketfile=new BufferedReader(new FileReader(new File(bucketFilename)));
	try {
		outputWriter=new FileWriter(new File(outputFilename),true);
	} catch (IOException e1) {
		e1.printStackTrace();
	}
	String line=null;
	try {
		while((line = bucketfile.readLine()) != null)
		{
			if(!recordsbucket.contains(line))
				recordsbucket.add(line);
		}
	} catch (IOException e1) {
		e1.printStackTrace();
	}	
	Iterator myiterator=recordsbucket.iterator();
	while(myiterator.hasNext())
	{
		try {
			outputWriter.write(myiterator.next()+"\n");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}	
	try {
		outputWriter.close();
	} catch (IOException e) {
		e.printStackTrace();
	}
	if(!new File(bucketFilename).delete()) {
		System.err.println("Error in deletion...");
	}
	
	}

private static void Accessrelation(RandomAccessFile rafPointer,int noOfBuffers,int BlockSize) throws IOException{
	
	List <String> recordsInSingleBlock=new ArrayList<String>();
	
	while((recordsInSingleBlock=getNext(rafPointer,BlockSize))!=null)
	{
		int totalRecordsInSingleBlock=recordsInSingleBlock.size();
		if(totalRecordsInSingleBlock==0)
			break;		
	
		//apply hashing and distribute the records in different files according to first character
		
		for(int i=0;i<totalRecordsInSingleBlock;i++)
		{
			char startingCharacter=recordsInSingleBlock.get(i).toLowerCase().charAt(0);
			if(buckets.get(startingCharacter)==null){
				buckets.put(startingCharacter,new HashSet<String>());
			}
			else
			{
				buckets.get(startingCharacter).add(recordsInSingleBlock.get(i));
				int BucketSize=buckets.get(startingCharacter).size();
				
				//write it to the file
				Iterator<String> myiterator;
				//=buckets.get(startingCharacter).iterator();
				if(BucketSize>=noOfRecordsInM)
				{
					myiterator=buckets.get(startingCharacter).iterator();
					String bucketfilename=startingCharacter+".text";
					if(!bucketFileNames.contains(bucketfilename))
					{
						bucketFileNames.add(bucketfilename);
					}
					BufferedWriter Bufferbucket=new BufferedWriter(new FileWriter(new File(bucketfilename),true));
					while(myiterator.hasNext())
					{
						Bufferbucket.write(myiterator.next()+"\n");
					}
					Bufferbucket.close();
					buckets.get(startingCharacter).clear();
				}
				
			}
		}	
	}
	Iterator<Map.Entry<Character,HashSet<String>>> myit=buckets.entrySet().iterator();
	
	while (myit.hasNext()) 
	{
	    Map.Entry<Character, HashSet<String>> entry = myit.next();
		String bucketFileName = entry.getKey() + "_bucket";
		
		if(!bucketFileNames.contains(bucketFileName)) {
			bucketFileNames.add(bucketFileName);
		}
	    BufferedWriter bfrBucket = new BufferedWriter(new FileWriter(new File(bucketFileName), true));
	    Iterator<String> itr2 = entry.getValue().iterator();
	    while (itr2.hasNext()) {
	    	bfrBucket.write(itr2.next() + "\n");
	    }
	    bfrBucket.close();
	    entry.getValue().clear();
	}
	
}

private static List <String> getNext(RandomAccessFile rafPointer, int BlockSize) throws IOException {
	
	List <String> loadRecordsOfSizeM = new ArrayList<String>();
	String line = null;
	
	rafPointer.seek(currentFilePointerPosition);
	
	while((line = rafPointer.readLine()) != null) {
		loadRecordsOfSizeM.add(line);
		currentFilePointerPosition += line.length() + 1;
	}
	return loadRecordsOfSizeM;
}


	private static void UnionBtree(String table1,String table2,int NUmberOfAttributes,double BlockSize,int RecordSize)
	{
		
	}
}
