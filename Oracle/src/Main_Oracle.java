import oracle.OracleInsert;
import oracle.OracleSearch;
import oracle.Parameters;
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
        else
            var = -1;

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

        switch (variable) {
            case -3:                    // Just insert III
                int indexed = var * Parameters.INSERT_INDEXED_FIELDS_FACTOR;
                or.just_insert_indexed_fields(indexed);
                break;
            case -2:                    // Just insert II
                int text_fields = var * Parameters.INSERT_FIELDS_FACTOR;          // Campos de texto
                or.just_insert_text_fields(text_fields, scenario);
                break;
            case -1:                        // Just insert
                int batches = var;          // batches
                or.just_insert(batches, Parameters.NREGISTERS);
                break;
            case 0:                                 // Batches
                int max_batches;
                switch (scenario) {
                    case 1:  max_batches = 2500; break;
                    case 2:  max_batches = 2000; break;
                    default: max_batches = 1000;
                }
                or.insert_batches(max_batches);
                break;
            case 1:                                 // Registers
                int min_regs = 1000;
                int max_regs = 150000;
                or.insert_registers(min_regs, max_regs, scenario);
                break;
            case 2:                                 // Text Fields
                // Limite de colunas do OracleInsert = 1000 - 5 INTS - 1 ID
                or.insert_text_fields(Parameters.INSERT_MAXTEXTFIELDS, scenario);
                break;
            case 3:                                 // Int Fields
                // Limite de colunas do OracleInsert = 1000 - 20 Colunas de Texto - 1 ID
                or.insert_int_fields(Parameters.INSERT_MAXINTFIELDS, scenario);
                break;
            case 4:                                 // Indexed Fields
                // Limite de colunas do OracleInsert = 1000 - 5 INTS - 1 ID
                or.insert_indexed_fields(Parameters.INSERT_MAXTEXTFIELDS);
                break;
        }
    }

    private static void search() throws Exception {
        int ind;
        switch (scenario) {                 // Indexed fields
            case 1:  ind = 1; break;
            case 2:  ind = 10; break;
            default: ind = 20;
        }

        OracleSearch oracle = new OracleSearch(ind);
        switch (variable) {
            case 0:
                int textfields = Parameters.TEXT;
                if (var != -1)
                    textfields = var * Parameters.INSERT_FIELDS_FACTOR;
                
                oracle.search_seconds(textfields, scenario);
                break;
            case 2:
                int min_ops = 10;
                int max_ops;
                switch (scenario) {
                    case 1:  max_ops = 2000; break;
                    case 2:  max_ops = 700; break;
                    default: max_ops = 500;
                }
                oracle.search_opspersec(min_ops, max_ops);
                break;
            case 3:
                oracle.search_results(Parameters.SEARCH_MAXRESULTS);
                break;
            case 4:
                int bd_text_fields = var * Parameters.INSERT_INDEXED_FIELDS_FACTOR;
                oracle.search_indexed(bd_text_fields);
                break;
        }
    }
}
