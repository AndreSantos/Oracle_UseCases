
import oracle.OracleInsert;
import oracle.OracleSearch;

// Compile: javac -cp .:lib/ojdbc6.jar Main_Oracle.java OracleInsert.java

// INSERT:
    // Run:  java -Xmx1536M -Xms1024M -cp .:lib/ojdbc6.jar OracleInsert insert 1 1

public class Main_Oracle {
    static String use_case;
    static int scenario, variable;
    static int var;
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("usage: java -Xmx1536M -Xms1024M Oracle <use_case> <variable> <scenario> [var1]");
            return;
        }

        use_case = args[0];
        variable = Integer.valueOf(args[1]);
        scenario = Integer.valueOf(args[2]);
        if (args.length > 3)
            var  = Integer.valueOf(args[3]);

        if (use_case.contentEquals("insert")) {
            insert();
        }
        if (use_case.contentEquals("search")) {
            search();
        }
    }

    private static void insert() throws Exception {
        String [] vec = new String[30];
        vec[0] = "1";                       // Batches
        vec[1] = "200";                     // Registers
        vec[2] = "20";                      // Text Fields
        vec[3] = "20";                      //      Field Length
        vec[4] = "5";                       //      Int Fields
        vec[5] = "0";                       //      Float Fields    (=0)

        switch (scenario) {                 // Indexed fields
            case 1:  vec[6] = "1"; break;
            case 2:  vec[6] = "10"; break;
            default: vec[6] = "20";
        }

        vec[7] = "S";                      // Dictionary
        vec[8] = "2";                      // Mode

        oracle.OracleInsert or = new oracle.OracleInsert(vec);

        if (scenario == 0) {
            if (or.exist_table())
                OracleInsert.drop_table();
            return;
        }

        int beg, end;
        switch (variable) {
            case -1:                        // Just insert
                beg = var;          // batches
                end = 5000;         // registers
                or.just_insert(beg, end);
                break;
            case 0:                                 // Batches
                switch (scenario) {
                    case 1:  end = 2500; break;
                    case 2:  end = 1200; break;
                    default: end = 500;
                }
                or.insert_batches(end);
                break;
            case 1:                                 // Registers
                beg = 1000;
                end = 150000;
                or.insert_registers(beg, end, scenario);
                break;
            case 2:                                 // Text Fields
                end = 990;  // Limite de colunas do OracleInsert = 1000
                or.insert_text_fields(end, scenario);
                break;
            case 3:                                 // Int Fields
                end = 970;  // Limite de colunas do OracleInsert = 1000
                or.insert_int_fields(end, scenario);
                break;
            case 4:                                 // Indexed Fields
                end = 990;  // Limite de colunas do OracleInsert = 1000
                or.insert_indexed_fields(end);
                break;
        }
    }

    private static void search() throws Exception {
        String [] vec = new String[30];
        vec[0] = "100";                     // Ops per sec
        vec[1] = "10";                      // Num Threads
        vec[3] = "10";                      // Seconds
        vec[4] = "20";                      // Field Length
        vec[5] = "S";                       // Dictionary
        vec[6] = "20";                      // Text Fields
        vec[7] = "5";                       // Int Fields
        
        vec[2] = "20";                      // Num Words    - Deprecated


        switch (scenario) {                 // Indexed fields
            case 1:  vec[8] = "1"; break;
            case 2:  vec[8] = "10"; break;
            default: vec[8] = "20";
        }
        vec[9] = "10";                       // Resultados

        OracleSearch oracle = new OracleSearch(vec);
        
        switch (variable) {
            case 0:
                int seconds = 10;
                oracle.search_seconds(seconds);
                break;
            case 1:
                int threads = 200;
                oracle.search_threads(threads);
                break;
            case 2:
                int ops = 200;
                oracle.search_threads(ops);
                break;
            case 3:
                int results = 250;
                oracle.search_results(results);
                break;
        }
    }
}
