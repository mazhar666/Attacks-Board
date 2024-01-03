package dataGenerator;
import java.io.*;

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
}
