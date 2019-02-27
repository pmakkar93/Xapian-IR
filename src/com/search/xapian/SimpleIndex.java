package com.search.xapian;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import org.xapian.*;

public class SimpleIndex {

    public static RandomAccessFile in;
    public static ArrayList<String> inputFiles;
    public static ArrayList<String> searchTerms;
    public static String inputPath=null;
    public static String searchFilePath=null;
//    static int NUMTHREADS= Runtime.getRuntime().availableProcessors();
    static int NUMTHREADS = 0;
    static int fileNumChunk = 0;
    static double indexTime = 0.0;
    static long indexSize = 0;
    static double searchTime = 0.0;
    private static double sizeOfInputFiles = 0.0;
    static long start=0,end=0;
    static double indexThroughput = 0.0;
    static double queryThroughput = 0.0;
    static int percentCompletion = 10;
    static long beforeUsedMem = 0;

    // 0- input file path, 1- Number of threads, 2 - Number of File chunk, 3- search file path 
    public static void main(String[] args) throws Exception {
    	// /home/pranav/Desktop/Xapian/xapian-bindings-1.2.25/java/Test_folder/TESTING.dat
    	inputPath = args[0];
    	System.out.println("Input file path: " + inputPath);
    	
    	NUMTHREADS = Integer.parseInt(args[1]);
//    	NUMTHREADS = Runtime.getRuntime().availableProcessors();
      	System.out.println("Number of Threads : " + NUMTHREADS);

      	fileNumChunk = Integer.parseInt(args[2]);
      	System.out.println("File Chunk : " + fileNumChunk);
      	
      	searchFilePath = args[3]; 
    	System.out.println("Search file path: " + searchFilePath);
    	    	
            // read the file paths from the input file
            inputFiles = new ArrayList<String>();
            in = new RandomAccessFile(inputPath, "r");
            String line;
            
			while ((line = in.readLine()) != null ) {
                inputFiles.add(line);
            }
            in.close();
            
            // read the file paths from the input file
            searchTerms = new ArrayList<String>();
            in = new RandomAccessFile(searchFilePath, "r");
            String line1;
			while ((line1 = in.readLine()) != null ) {
				searchTerms.add(line1);
            }
            in.close();            
            System.gc();
            
            System.out.println("Number of files in Input file is : " + inputFiles.size());
            System.out.println("Number of files to Index is : " + fileNumChunk);
            
        	// Writing in memory db
        	System.out.println("Writing the Input files in in-Memory DB");

            // create or *overwrite an existing* Xapian database
            ArrayList<WritableDatabase> arrDb = new ArrayList<WritableDatabase>();

            printUsage();
            
            start = System.currentTimeMillis();
            
            // Writing all files one by one in Database using multiple threads            
       	 	Thread[] threads = new Thread[NUMTHREADS];
	       	 for (int i = 0; i < threads.length; i++) {
	       		MyRunnable mr = new MyRunnable(inputFiles,i,NUMTHREADS,fileNumChunk); 
	    		Runnable worker = mr;
	 	       	Thread t = new Thread(worker,"" + i);
	 	       	threads[i] = t;
		 	       	t.start();
		 	       	arrDb.add(mr.getDB());
	       	 }
	       	try {
            	for(Thread th : threads) {
            		th.join();
            	}
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

	       	// Removing unnecessary objects
	       	threads = null;
            System.gc();

		       	WritableDatabase wdfinal = InMemory.open();
		       	// make sure to flush the database so the documents get written to disk
	       		int count=0;
		       	for (WritableDatabase wdb : arrDb) {
			       	wdfinal.addDatabase(wdb);
			       	wdfinal.commit();
		       		System.out.println("COUNT of DOCUMENTS in DB "+ ++count +" is " + wdb.getDocCount());
		       	}	       	
		       	wdfinal.flush();

	       	end = System.currentTimeMillis();
	       	
	       	// Cleaning unused data
	       	arrDb = null;
            System.gc();
                        
            // calculate the time taken to index the files
            indexTime = (end - start);
            
	        // SEARCHING THE DB 
		        
		        QueryParser queryp = new QueryParser();
		        queryp.setStemmer(new Stem("english"));
		        
		        // open the specified database
		        queryp.setDatabase(wdfinal);
		       
		        start = System.currentTimeMillis();
		        // turn the remaining command-line arguments into our query
//		        Query query=null;
		        for (String s : searchTerms){
		        	Query query = queryp.parseQuery(s);
			        // Searching the database
			        start = System.currentTimeMillis();
			        searching(query,wdfinal);
		            end = System.currentTimeMillis();
		            System.out.println("Time taken for searching the term " + s + " : " + (end - start) + " ms");
		            searchTime = searchTime + (end - start);
		        }	            
		        // calculate the time taken to search a normal query

	            indexThroughput =(sizeOfInputFiles/(1024.0*1024.0))/(indexTime/1000.0);
	            indexThroughput =Double.parseDouble(new DecimalFormat("##.###").format(indexThroughput));

	            queryThroughput =(sizeOfInputFiles/(1024.0*1024.0))/(searchTime/1000.0);
	            queryThroughput =Double.parseDouble(new DecimalFormat("##.###").format(queryThroughput));
	            
	            sizeOfInputFiles = sizeOfInputFiles/(1024*1024);
	            sizeOfInputFiles = Double.parseDouble(new DecimalFormat("##.##").format(sizeOfInputFiles));
	            
		        System.out.println("#################################");           
	            System.out.println("Details about the writing Index ");
		       	System.out.println("Number of Documents in Final DB : " + wdfinal.getDocCount() );
		       	System.out.println("Total Input files size : " + sizeOfInputFiles + " MB");
	            System.out.println("Index Writing time : " + indexTime + " ms");
	            System.out.println("Total Indexing Throughput : " + indexThroughput + " Mbps");
		       	System.out.println("DB Index Size: " + (wdfinal.getTotalLength() * 245)/(1024*1024*10) + " MB");
		       	System.out.println("Total Search Terms : " + searchTerms.size());
	            System.out.println("Searching time : " + searchTime + "ms");
	            System.out.println("Total Query Throughput : " + queryThroughput + " Mbps");
		       	System.out.println("Avg Query search time : " + searchTerms.size()/searchTime + " query/ms");
		       	System.out.println("#################################"); 
		       	
	            printUsage();
	            
		       	// Cleaning all unused items
		       	inputFiles.clear();
		       	searchTerms.clear();
	            System.gc();		       	
    }

    public static class MyRunnable implements Runnable {
    	private WritableDatabase db = InMemory.open();
    	private ArrayList<String> inputFiles = new ArrayList<String>();
    	private int thread=0;
    	private int totalNumThreads = 0;
    	private int fileNumChunk = 0;
        MyRunnable(ArrayList<String> inputFiles, int thread, int totalNumThreads,int fileNumChunk){
        	this.inputFiles=inputFiles;
        	this.thread=thread;
        	this.totalNumThreads=totalNumThreads;
        	this.fileNumChunk = fileNumChunk;
        }
        @Override
		public void run() {
        	this.db = indexing(inputFiles,thread,db,totalNumThreads,fileNumChunk);
		}
        public WritableDatabase getDB() {
        	this.db.flush();
        	return this.db;
        }
    }

    public static WritableDatabase indexing(ArrayList<String> inputArr,int numThread,WritableDatabase db,int totalNumThreads,int fileNumChunk){
        String input=null;
        int size1 =0;
        if(fileNumChunk<=inputArr.size()){
        	size1 = fileNumChunk;
        }
        else{
        	size1 = inputArr.size();
        }
    	try {	        
            for (int i=numThread; i<fileNumChunk && i<inputArr.size(); i=i+totalNumThreads) {
	        	//System.out.println("Writing file name : " + i + "  " + inputArr.get(i) + " Thread Number : "+ numThread);
            	if(i == (size1*percentCompletion)/100 && percentCompletion <= 100 ){
            		System.out.println("Phase Completion : " + percentCompletion + "%");
            		percentCompletion = percentCompletion + 10;
            	}
            	
	        	input = inputArr.get(i);
	        
	        	// Loading the content
	        	File f = new File(input);
	            List<String> content = Files.readAllLines(Paths.get(input), StandardCharsets.ISO_8859_1);
	            
	            // Initialize indexer
	            TermGenerator indexer = new TermGenerator();
	            // Set word stemmer to English
//	            indexer.setStemmer(new Stem("english"));
	            
	            // Prepare document
	            Document document = new Document();
	            // Adding file name in document
	            document.addValue(0, f.getName().getBytes());
	            
	            // Getting size of Input files while writing into index
	            sizeOfInputFiles= sizeOfInputFiles + (f.length()-640097);
	            
	            // Index document  
	            indexer.setDocument(document);
	            for (String line : content) {
	                String[] stringArray = line.split(" ");        
	                //Indexing only relevant columns - 3,4,8,9,10,11,12,14,15
	                String indexLine = stringArray[3] + " " + stringArray[4] + " " + stringArray[8] + " " + stringArray[9] + " " + stringArray[10] + " " + stringArray[11] + " " + stringArray[12] + " " + stringArray[14]+ " " + stringArray[15];
	                //System.out.println(indexLine);
	            	indexer.indexTextWithoutPositions(indexLine);
	            }
	            // Store the indexed content into the database
	            db.addDocument(document);
	            // Cleaning unused data
	            content = null;
	            document = null;
	            indexer = null;
	            f = null;
	            System.gc();
	            
	        }
            return db;
        } catch (IOException e) {
            e.printStackTrace();
        }
       return InMemory.open();
    }
    
    public static void searching(Query query,WritableDatabase db) {
        // and query the database
        Enquire enquire = new Enquire(db);
        enquire.setQuery(query);
        MSet matches = enquire.getMSet(0, 2500);    // get up to 2500 matching documents
        MSetIterator itr = matches.begin();

        System.err.println("Found " + matches.size() + " matching documents using " + query.toString());
        
        int counter = 10;
        while (itr.hasNext()) {
          // by returning some kind of "MatchDescriptor" object
          Document doc = itr.getDocument();
  //        System.err.println(itr.getPercent() + "%  Ranking: " + itr.getRank() + " DOCUMENT ID: " + doc.getDocId() + " File name : " +  new String(doc.getValue(0)));
          long dummy = itr.next();
//          counter--;
      }
    }
    
    public static byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }

    public static long bytesToLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes);
        buffer.flip();//need flip 
        return buffer.getLong();
    }
    private static void printUsage() {
    	  OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();
    	  for (Method method : operatingSystemMXBean.getClass().getDeclaredMethods()) {
    	    method.setAccessible(true);
    	    if (method.getName().startsWith("get")
    	        && Modifier.isPublic(method.getModifiers())) {
    	            Object value;
    	        try {
    	            value = method.invoke(operatingSystemMXBean);
    	        } catch (Exception e) {
    	            value = e;
    	        } 
    	        System.out.println("CPU AND MEMORY USAGE: " + method.getName() + " = " + value);
    	    }
    	  } 
    	}
}

