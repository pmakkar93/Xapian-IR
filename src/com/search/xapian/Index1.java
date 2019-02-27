package com.search.xapian;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.xapian.Document;
import org.xapian.Stem;
import org.xapian.TermGenerator;
import org.xapian.WritableDatabase;
import org.xapian.XapianConstants;
import org.xapian.XapianJNI;

public class Index1 {
    
    // Command line args - dbpath datapath
    public static void main(String[] args)
    {
        if(args.length < 2)
        {
            System.out.println("Insufficient number of arguments (should be dbpath datapath)");
            return;
        }
        index(args[1], args[0]);
    }
    
    public static void index(String datapath, String dbpath)
    {
        // Create or open the database we're goign to be writing to.
        WritableDatabase db = new WritableDatabase(dbpath, XapianConstants.DB_CREATE_OR_OPEN);
        
        // Set up a TermGenerator that we'll use in indexing.
        TermGenerator termGenerator = new TermGenerator();
        termGenerator.setStemmer(new Stem("en"));
        
        //Parsing the CSV input file
        Scanner csvScanner,lineScanner;        
        csvScanner = lineScanner = null;
        
        try {
            File csv = new File(datapath);
            csvScanner = new Scanner(csv);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Index1.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        //Ignoring first line (contains descriptors)
        csvScanner.nextLine();
        
        while(csvScanner.hasNextLine())
        {
            String currentLine = csvScanner.nextLine();
            lineScanner = new Scanner(currentLine);
            lineScanner.useDelimiter(",");
            
            /* Parsing each line for identifier, title, and description */
            
            //Identifier is the first comma seperated value (according to CSV file)
            String identifier = lineScanner.next();
            
            //Title is third comma seperated value
            lineScanner.next();
            String title = lineScanner.next();
            byte[] title1 = title.getBytes();
            
            //Description is ninth comma sperated value
            for(int i=0;i<5;i++)
                   lineScanner.next();
            String description = lineScanner.next();
            
            /* Finished Parsing line */
            
            // We make a document and tell the term generator to use this.
            Document doc = new Document();
            termGenerator.setDocument(doc);
            
            // Index each field with a suitable prefix.
            termGenerator.indexText(title, 1, "S");
            termGenerator.indexText(description, 1, "XD");
            
            // Index fields without prefixes for general search.
            termGenerator.indexText(title);
            termGenerator.increaseTermpos();
            termGenerator.indexText(description);
            
            // Store all fields for display purposes
            doc.setData(currentLine);
            doc.addValue(0, title1);
            
            // We use the identifier to ensure each object ends up in the
            // database only once no matter how many times we run the
            // indexer.
            String idterm = "Q"+identifier;
            doc.addBooleanTerm(idterm);
            db.replaceDocument(idterm, doc);
        }
        
        // Commit to write documents to disk
        db.commit();

        lineScanner.close();
        csvScanner.close();        
    }
}
