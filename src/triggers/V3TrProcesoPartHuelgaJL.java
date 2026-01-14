package triggers;

import org.h2.api.Trigger;
import java.sql.Connection;

public class V3TrProcesoPartHuelgaJL implements Trigger {

    // ===== Column indexes (ADJUST TO YOUR TABLE ORDER) =====
    private static final int ACTOR = 1;
    private static final int DEFENSA_ACT = 2;

    private static final int NOMBRE_SINDICATO = 3;
    private static final int REG_ASOC_SINDICAL = 4;
    private static final int TIPO_SINDICATO = 5;
    private static final int OTRO_ESP_SINDICATO = 6;

    private static final int ORG_OBRERA = 7;
    private static final int NOMBRE_ORG_OBRERA = 8;
    private static final int OTRO_ESP_OBRERA = 9;

    @Override
    public void init(Connection conn, String schemaName, String triggerName,
                     String tableName, boolean before, int type) {
        // no-op
    }

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) {

        int actor = toInt(newRow[ACTOR], 0);
        int tipoSindicato = toInt(newRow[TIPO_SINDICATO], 0);
        int orgObrera = toInt(newRow[ORG_OBRERA], 0);
        int nombreOrgObrera = toInt(newRow[NOMBRE_ORG_OBRERA], 0);

        // ===== Defaults =====
        if (newRow[ACTOR] == null)
            newRow[ACTOR] = 99;

        if (newRow[DEFENSA_ACT] == null)
            newRow[DEFENSA_ACT] = 9;

        // ===== Actor = Sindicato (3) =====
        if (actor == 3) {

            if (newRow[NOMBRE_SINDICATO] == null)
                newRow[NOMBRE_SINDICATO] = "No Especifico";

            if (newRow[REG_ASOC_SINDICAL] == null)
                newRow[REG_ASOC_SINDICAL] = "No Especifico";

            if (newRow[TIPO_SINDICATO] == null)
                newRow[TIPO_SINDICATO] = 9;

            if (newRow[ORG_OBRERA] == null)
                newRow[ORG_OBRERA] = 9;
        }

        // ===== Sindicato details =====
        if (tipoSindicato == 6 && newRow[OTRO_ESP_SINDICATO] == null)
            newRow[OTRO_ESP_SINDICATO] = "No Especifico";

        if (orgObrera == 1 && newRow[NOMBRE_ORG_OBRERA] == null)
            newRow[NOMBRE_ORG_OBRERA] = 9;

        if (nombreOrgObrera == 8 && newRow[OTRO_ESP_OBRERA] == null)
            newRow[OTRO_ESP_OBRERA] = "No Especifico";
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
