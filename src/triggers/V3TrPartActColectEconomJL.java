package triggers;

import org.h2.api.Trigger;

import java.sql.Connection;

public class V3TrPartActColectEconomJL implements Trigger {

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

    private static final int TIPO = 10;

    private static final int RFC_PATRON = 11;
    private static final int RAZON_SOCIAL_EMPR = 12;
    private static final int CALLE = 13;
    private static final int N_EXT = 14;
    private static final int N_INT = 15;
    private static final int COLONIA = 16;
    private static final int CP = 17;

    private static final int ENTIDAD_NOMBRE_EMPR = 18;
    private static final int ENTIDAD_CLAVE_EMPR = 19;
    private static final int MUNICIPIO_NOMBRE_EMPR = 20;
    private static final int MUNICIPIO_CLAVE_EMPR = 21;

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
        int tipo = toInt(newRow[TIPO], 0);

        // ===== Defaults =====
        if (newRow[ACTOR] == null) newRow[ACTOR] = 99;
        if (newRow[DEFENSA_ACT] == null) newRow[DEFENSA_ACT] = 9;

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

            if (orgObrera == 1 && nombreOrgObrera == 8
                    && newRow[OTRO_ESP_OBRERA] == null)
                newRow[OTRO_ESP_OBRERA] = "No Especifico";
        }

        // ===== Actor = Patrón (2) =====
        if (actor == 2) {

            if (newRow[TIPO] == null)
                newRow[TIPO] = 9;

            if (tipo == 1 && newRow[RFC_PATRON] == null)
                newRow[RFC_PATRON] = "No Identificado";

            if (tipo == 2) {

                if (newRow[RAZON_SOCIAL_EMPR] == null)
                    newRow[RAZON_SOCIAL_EMPR] = "No Identificado";

                if (newRow[CALLE] == null)
                    newRow[CALLE] = "No Identificado";

                if (newRow[N_EXT] == null)
                    newRow[N_EXT] = "No Identificado";

                if (newRow[N_INT] == null)
                    newRow[N_INT] = "No Identificado";

                if (newRow[COLONIA] == null)
                    newRow[COLONIA] = "No Identificado";

                if (newRow[CP] == null)
                    newRow[CP] = "No Identificado";

                if (newRow[ENTIDAD_NOMBRE_EMPR] == null)
                    newRow[ENTIDAD_NOMBRE_EMPR] = "No Identificado";

                if (newRow[ENTIDAD_CLAVE_EMPR] == null)
                    newRow[ENTIDAD_CLAVE_EMPR] = 99;

                if (newRow[MUNICIPIO_NOMBRE_EMPR] == null)
                    newRow[MUNICIPIO_NOMBRE_EMPR] = "No Identificado";

                if (newRow[MUNICIPIO_CLAVE_EMPR] == null)
                    newRow[MUNICIPIO_CLAVE_EMPR] = 99999;
            }
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
