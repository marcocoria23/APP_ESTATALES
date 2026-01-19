package triggers;

import org.h2.api.Trigger;

import java.sql.Connection;
import java.sql.SQLException;

public class V3TrPartDemOrdinarioJL implements Trigger {

    @Override
    public void init(Connection conn, String schemaName, String triggerName,
                     String tableName, boolean before, int type) {
        // no-op
    }

    private Integer asInt(Object v) {
        if (v == null) return null;
        if (v instanceof Number) return ((Number) v).intValue();
        String s = v.toString().trim();
        if (s.isEmpty()) return null;
        return Integer.valueOf(s);
    }

    private void setIfNull(Object[] newRow, int idx, Object value) {
        if (newRow[idx] == null) newRow[idx] = value;
    }

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {
try {
        // ===== Índices (0-based) según tu CREATE TABLE =====
        // 0  NOMBRE_ORGANO_JURIS
        // 1  CLAVE_ORGANO
        // 2  EXPEDIENTE_CLAVE
        // 3  ID_DEMANDADO
        final int iDEMANDADO = 4;
        final int iDEFENSA_DEM = 5;
        final int iTIPO = 6;
        final int iRFC_PATRON = 7;
        final int iRAZON_SOCIAL_EMPR = 8;
        final int iCALLE = 9;
        final int iN_EXT = 10;
        final int iN_INT = 11;
        final int iCOLONIA = 12;
        final int iCP = 13;
        final int iENTIDAD_NOMBRE_EMPR = 14;
        final int iENTIDAD_CLAVE_EMPR = 15;
        final int iMUNICIPIO_NOMBRE_EMPR = 16;
        final int iMUNICIPIO_CLAVE_EMPR = 17;
        final int iLATITUD_EMPR = 18;
        final int iLONGITUD_EMPR = 19;
        // 20 COMENTARIOS

        // ===== Defaults base =====
        // if DEMANDADO is null -> 9
        setIfNull(newRow, iDEMANDADO, 9);

        // if DEFENSA_DEM is null -> 9
        setIfNull(newRow, iDEFENSA_DEM, 9);

        Integer demandado = asInt(newRow[iDEMANDADO]);

        // ===== Demandado = 1 =====
        if (demandado != null && demandado == 1) {

            // if TIPO is null -> 9
            setIfNull(newRow, iTIPO, 9);

            Integer tipo = asInt(newRow[iTIPO]);

            // RFC_PATRON default cuando Demandado=1 y Tipo=1 o 2
            if (tipo != null && (tipo == 1 || tipo == 2)) {
                setIfNull(newRow, iRFC_PATRON, "No identificado");
            }

            // Si Tipo = 2: llenar domicilio/ubicación
            if (tipo != null && tipo == 2) {
                setIfNull(newRow, iCALLE, "No identificado");
                setIfNull(newRow, iN_EXT, "No identificado");
                setIfNull(newRow, iN_INT, "No identificado");
                setIfNull(newRow, iCOLONIA, "No identificado");
                setIfNull(newRow, iCP, "No identificado");

                setIfNull(newRow, iENTIDAD_NOMBRE_EMPR, "No identificado");
                setIfNull(newRow, iENTIDAD_CLAVE_EMPR, 99);

                setIfNull(newRow, iMUNICIPIO_NOMBRE_EMPR, "No identificado");
                setIfNull(newRow, iMUNICIPIO_CLAVE_EMPR, 99999);

                setIfNull(newRow, iLATITUD_EMPR, "No identificado");
                setIfNull(newRow, iLONGITUD_EMPR, "No identificado");
            }

            // Nota: en tu trigger Oracle NO pones default para RAZON_SOCIAL_EMPR,
            // por eso aquí NO lo llené (aunque existe la columna).
        }
 } catch (Exception e) {
            System.out.println("EXCEPCION en trigger: " + e.getClass().getName() + " - " + e.getMessage());
            e.printStackTrace(System.out); // <-- AQUI veras la linea exacta
            throw e; // <-- importante: no te comas el error
        }

}

    @Override public void close() { }
    @Override public void remove() { }
}
