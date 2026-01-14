package triggers;

import org.h2.api.Trigger;
import java.sql.Connection;

public class V3TrPartDemOrdinarioJL implements Trigger {

    // ===== Column indexes (AJUSTAR AL ORDEN REAL DE LA TABLA) =====
    private static final int DEMANDADO = 1;
    private static final int DEFENSA_DEM = 2;
    private static final int TIPO = 3;

    private static final int RFC_PATRON = 4;
    private static final int CALLE = 5;
    private static final int N_EXT = 6;
    private static final int N_INT = 7;
    private static final int COLONIA = 8;
    private static final int CP = 9;
    private static final int ENTIDAD_NOMBRE_EMPR = 10;
    private static final int ENTIDAD_CLAVE_EMPR = 11;
    private static final int MUNICIPIO_NOMBRE_EMPR = 12;
    private static final int MUNICIPIO_CLAVE_EMPR = 13;
    private static final int LATITUD_EMPR = 14;
    private static final int LONGITUD_EMPR = 15;

    @Override
    public void init(Connection conn, String schemaName, String triggerName,
                     String tableName, boolean before, int type) {
        // no-op
    }

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) {

        int demandado = toInt(newRow[DEMANDADO], 0);
        int tipo = toInt(newRow[TIPO], 0);

        // ===== Defaults generales =====
        if (newRow[DEMANDADO] == null)
            newRow[DEMANDADO] = 9;

        if (newRow[DEFENSA_DEM] == null)
            newRow[DEFENSA_DEM] = 9;

        // ===== Demandado: Patrón =====
        if (demandado == 1) {

            if (newRow[TIPO] == null)
                newRow[TIPO] = 9;

            // RFC obligatorio (Tipo 1 y 2)
            if (newRow[RFC_PATRON] == null)
                newRow[RFC_PATRON] = "No identificado";

            // ===== Tipo de patrón = Empresa =====
            if (tipo == 2) {

                if (newRow[CALLE] == null)
                    newRow[CALLE] = "No identificado";

                if (newRow[N_EXT] == null)
                    newRow[N_EXT] = "No identificado";

                if (newRow[N_INT] == null)
                    newRow[N_INT] = "No identificado";

                if (newRow[COLONIA] == null)
                    newRow[COLONIA] = "No identificado";

                if (newRow[CP] == null)
                    newRow[CP] = "No identificado";

                if (newRow[ENTIDAD_NOMBRE_EMPR] == null)
                    newRow[ENTIDAD_NOMBRE_EMPR] = "No identificado";

                if (newRow[ENTIDAD_CLAVE_EMPR] == null)
                    newRow[ENTIDAD_CLAVE_EMPR] = 99;

                if (newRow[MUNICIPIO_NOMBRE_EMPR] == null)
                    newRow[MUNICIPIO_NOMBRE_EMPR] = "No identificado";

                if (newRow[MUNICIPIO_CLAVE_EMPR] == null)
                    newRow[MUNICIPIO_CLAVE_EMPR] = 99999;

                if (newRow[LATITUD_EMPR] == null)
                    newRow[LATITUD_EMPR] = "No identificado";

                if (newRow[LONGITUD_EMPR] == null)
                    newRow[LONGITUD_EMPR] = "No identificado";
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
