package triggers;

import org.h2.api.Trigger;
import java.sql.Connection;

public class V3TrPartActOrdinarioJL implements Trigger {

    // ===== Column indexes (ADJUST TO YOUR TABLE ORDER) =====
    private static final int ACTOR = 1;
    private static final int DEFENSA_ACT = 2;

    // Trabajador
    private static final int SEXO = 3;
    private static final int EDAD = 4;
    private static final int OCUPACION = 5;
    private static final int NSS = 6;
    private static final int CURP = 7;
    private static final int RFC_TRABAJADOR = 8;
    private static final int JORNADA = 9;

    // Sindicato
    private static final int NOMBRE_SINDICATO = 10;
    private static final int REG_ASOC_SINDICAL = 11;
    private static final int TIPO_SINDICATO = 12;
    private static final int OTRO_ESP_SINDICATO = 13;
    private static final int ORG_OBRERA = 14;
    private static final int NOMBRE_ORG_OBRERA = 15;
    private static final int OTRO_ESP_OBRERA = 16;

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

        // ===== Global defaults =====
        if (newRow[DEFENSA_ACT] == null)
            newRow[DEFENSA_ACT] = 9;

        if (newRow[ACTOR] == null)
            newRow[ACTOR] = 99;

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

        // ===== Actor = Sindicato (3) =====
        if (actor == 3) {

            if (newRow[NOMBRE_SINDICATO] == null)
                newRow[NOMBRE_SINDICATO] = "No Identificado";

            if (newRow[REG_ASOC_SINDICAL] == null)
                newRow[REG_ASOC_SINDICAL] = "No Identificado";

            if (newRow[TIPO_SINDICATO] == null)
                newRow[TIPO_SINDICATO] = 9;

            if (tipoSindicato == 6 && newRow[OTRO_ESP_SINDICATO] == null)
                newRow[OTRO_ESP_SINDICATO] = "No Especifico";

            if (newRow[ORG_OBRERA] == null)
                newRow[ORG_OBRERA] = 9;

            if (orgObrera == 1 && newRow[NOMBRE_ORG_OBRERA] == null)
                newRow[NOMBRE_ORG_OBRERA] = 9;

            if (orgObrera == 1 && nombreOrgObrera == 8 && newRow[OTRO_ESP_OBRERA] == null)
                newRow[OTRO_ESP_OBRERA] = "No Especifico";
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
