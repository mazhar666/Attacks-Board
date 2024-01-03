package Utilz;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class CSVGenerator {
    private static final BufferedReader br;
    static {
        try {
            br = new BufferedReader(new FileReader("Cybersecurity_attacks.csv"));
        } catch (FileNotFoundException e) {
            System.out.print("file not found ");
            throw new RuntimeException(e);

        }
    }
    public static BufferedReader getScanner() {
        return br;
    }
    public static String getNextLine() throws IOException {
    String line ;
        if ((line = br.readLine()) != null){
              return line;
        }
        return null;
}}
