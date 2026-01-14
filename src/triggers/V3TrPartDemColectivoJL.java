package triggers;

import org.h2.api.Trigger;
import java.sql.Connection;

public class V3TrPartDemColectivoJL implements Trigger {

    // ===== Column indexes (ADJUST IF NEEDED) =====
    private static final int DEMANDADO = 1;
    private static final int DEFENSA_DEM = 2;

    // Sindicato demandado
    private static final int NOMBRE_SINDICATO_DEM = 3;
    private static final int REG_ASOC_SINDICAL_DEM = 4;
    private static final int TIPO_SINDICATO_DEM = 5;
    private static final int OTRO_ESP_SINDICATO_DEM = 6;
    private static final int ORG_OBRERA_DEM = 7;
    private static final int NOMBRE_ORG_OBRERA_DEM = 8;
    private static final int OTRO_ESP_OBRERA_DEM = 9;

    // Patrón demandado
    private static final int TIPO_DEM_PAT = 10;
    private static final int RFC_PATRON_DEM = 11;
    private static final int RAZON_SOCIAL_EMPR_DEM = 12;
    private static final int CALLE = 13;
    private static final int N_EXT = 14;
    private static final int N_INT = 15;
    private static final int COLONIA = 16;
    private static final int CP = 17;
    private static final int ENTIDAD_NOMBRE_EMPR = 18;
    private static final int ENTIDAD_CLAVE_EMPR = 19;
    private static final int MUNICIPIO_NOMBRE_EMPR = 20;
    private static final int MUNICIPIO_CLAVE_EMPR = 21;
    private static final int LATITUD_EMPR = 22;
    private static final int LONGITUD_EMPR = 23;

    @Override
    public void init(Connection conn, String schemaName, String triggerName,
                     String tableName, boolean before, int type) {
        // no-op
    }

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) {

        int demandado = toInt(newRow[DEMANDADO], 0);
        int tipoSindicatoDem = toInt(newRow[TIPO_SINDICATO_DEM], 0);
        int orgObreraDem = toInt(newRow[ORG_OBRERA_DEM], 0);
        int nombreOrgObreraDem = toInt(newRow[NOMBRE_ORG_OBRERA_DEM], 0);
        int tipoDemPat = toInt(newRow[TIPO_DEM_PAT], 0);

        // ===== Global defaults =====
        if (newRow[DEMANDADO] == null)
            newRow[DEMANDADO] = 9;

        if (newRow[DEFENSA_DEM] == null)
            newRow[DEFENSA_DEM] = 9;

        // ===== DEMANDADO = Sindicato (2) =====
        if (demandado == 2) {

            if (newRow[NOMBRE_SINDICATO_DEM] == null)
                newRow[NOMBRE_SINDICATO_DEM] = "No Identificado";

            if (newRow[REG_ASOC_SINDICAL_DEM] == null)
                newRow[REG_ASOC_SINDICAL_DEM] = "No Identificado";

            if (newRow[TIPO_SINDICATO_DEM] == null)
                newRow[TIPO_SINDICATO_DEM] = 9;

            if (tipoSindicatoDem == 6 && newRow[OTRO_ESP_SINDICATO_DEM] == null)
                newRow[OTRO_ESP_SINDICATO_DEM] = "No Especifico";

            if (newRow[ORG_OBRERA_DEM] == null)
                newRow[ORG_OBRERA_DEM] = 9;

            if (orgObreraDem == 1 && newRow[NOMBRE_ORG_OBRERA_DEM] == null)
                newRow[NOMBRE_ORG_OBRERA_DEM] = 9;

            if (orgObreraDem == 1 && nombreOrgObreraDem == 8
                    && newRow[OTRO_ESP_OBRERA_DEM] == null)
                newRow[OTRO_ESP_OBRERA_DEM] = "No especifico";
        }

        // ===== DEMANDADO = Patrón (1) =====
        if (demandado == 1) {

            if (newRow[TIPO_DEM_PAT] == null)
                newRow[TIPO_DEM_PAT] = 9;

            if (tipoDemPat == 1 && newRow[RFC_PATRON_DEM] == null)
                newRow[RFC_PATRON_DEM] = "No Identificado";

            if (tipoDemPat == 2) {

                if (newRow[RAZON_SOCIAL_EMPR_DEM] == null)
                    newRow[RAZON_SOCIAL_EMPR_DEM] = "No Identificado";

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

                if (newRow[LATITUD_EMPR] == null)
                    newRow[LATITUD_EMPR] = "No Identificado";

                if (newRow[LONGITUD_EMPR] == null)
                    newRow[LONGITUD_EMPR] = "No Identificado";
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
