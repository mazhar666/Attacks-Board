package Utilz;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class CSVGenerator {
    private static final BufferedReader br;
    private  static String splitBy = ",";
    static {
        try {
            br = new BufferedReader(new FileReader("Cybersecurity_attacks.csv"));
            br.readLine();
        } catch (FileNotFoundException e) {
            System.out.print("file not found ");
            throw new RuntimeException(e);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public static BufferedReader getScanner() {
        return br;
    }
    public static String[] getNextRow() throws IOException {
    String line ;
        if ((line=br.readLine()) != null){
              return line.split(splitBy);
        }
        return null;
}}
