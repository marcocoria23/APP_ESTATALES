package triggers;

import org.h2.api.Trigger;
import java.sql.Connection;

public class V3TrPartActIndividualJL implements Trigger {

    // ===== Column indexes (ADJUST TO YOUR TABLE ORDER) =====
    private static final int ACTOR = 1;
    private static final int DEFENSA_ACT = 2;
    private static final int SEXO = 3;
    private static final int EDAD = 4;
    private static final int OCUPACION = 5;
    private static final int NSS = 6;
    private static final int CURP = 7;
    private static final int RFC_TRABAJADOR = 8;
    private static final int JORNADA = 9;

    @Override
    public void init(Connection conn, String schemaName, String triggerName,
                     String tableName, boolean before, int type) {
        // no-op
    }

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) {

        int actor = toInt(newRow[ACTOR], 0);

        // ===== Defaults =====
        if (newRow[ACTOR] == null)
            newRow[ACTOR] = 99;

        if (newRow[DEFENSA_ACT] == null)
            newRow[DEFENSA_ACT] = 9;

        // ===== Actor = Trabajador (1) =====
        if (actor == 1) {

            if (newRow[SEXO] == null)
                newRow[SEXO] = 9;

            if (newRow[EDAD] == null)
                newRow[EDAD] = 99;

            if (newRow[OCUPACION] == null)
                newRow[OCUPACION] = 999;

            if (newRow[NSS] == null)
                newRow[NSS] = "No Identificado";

            if (newRow[CURP] == null)
                newRow[CURP] = "No Identificado";

            if (newRow[RFC_TRABAJADOR] == null)
                newRow[RFC_TRABAJADOR] = "No Identificado";

            if (newRow[JORNADA] == null)
                newRow[JORNADA] = 9;
        }
    }

    @Override public void close() {}
    @Override public void remove() {}

    // ===== Helper =====
    private static int toInt(Object v, int def) {
        if (v == null) return def;
        if (v instanceof Number) return ((Number) v).intValue();
        try { return Integer.parseInt(v.toString()); }
        catch (Exception e) { return def; }
    }
}
