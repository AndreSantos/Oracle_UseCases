// Compile: javac -cp .:lib/ojdbc6.jar Main.java Oracle.java

// INSERT:
    // Run:  java -Xmx1536M -Xms1024M -cp .:lib/ojdbc6.jar Oracle insert 1 1

public class Main {
    static String use_case, variable;
    static int scenario;
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.println("usage: java -Xmx1536M -Xms1024M Oracle <use_case> <variable> <scenario>");
            return;
        }

        use_case = args[0];
        variable = args[1];
        scenario = Integer.valueOf(args[2]);

        if (use_case.contentEquals("insert")) {
            insert();
        }
    }

    private static void insert() throws Exception {
        String [] vec = new String[30];
        vec[0] = "1";                       // Batches
        vec[1] = "1";                       // Registers
        vec[2] = "30";                      // Text Fields
        vec[3] = "20";                      //      Field Length
        vec[4] = "5";                       //      Int Fields
        vec[5] = "0";                       //      Float Length    (=0)

        switch (scenario) {                 // Indexed fields
            case 1:  vec[6] = "1"; break;
            case 2:  vec[6] = "15"; break;
            default: vec[6] = "30";
        }

        vec[7] = "S";                      // Dictionary
        vec[8] = "2";                      // Mode

        oracle.Oracle or = new oracle.Oracle(vec);
        int beg = 1000;
        int end = 150000;

        if (scenario == 0) {
            or.drop_table();
            return;
        }
        
        or.start_insert(beg, end);
    }
}
