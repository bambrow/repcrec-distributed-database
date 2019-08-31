import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * This is the main class to run our distributed database.
 *
 * @author Yichang Chen
 * Updated: 12/05/2018
 */
public class RepCRec {
    public static void main(String[] args) {

        TransactionManager transactionManager = new TransactionManager();

        if (args.length != 0) {
            System.out.println("Running file input mode: ");
            String inputPath = args[0];
            try {

                transactionManager.startFileMode(inputPath);
            } catch (Exception e) {
                System.out.println("Invalid input");
            }
        } else {
            System.out.println("running command line mode, start entering commands: ");
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(System.in));
            transactionManager.startCommandLineMode(reader);
        }
    }
}
