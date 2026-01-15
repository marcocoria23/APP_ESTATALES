package triggers;

import org.h2.api.Trigger;

import java.sql.Connection;
import java.sql.SQLException;

public class V3TrPartActIndividualJL implements Trigger {

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

        // ===== Índices (0-based) según tu DDL =====
        // 0  NOMBRE_ORGANO_JURIS
        // 1  CLAVE_ORGANO
        // 2  EXPEDIENTE_CLAVE
        // 3  ID_ACTOR
        final int iACTOR = 4;
        final int iDEFENSA_ACT = 5;
        final int iSEXO = 6;
        final int iEDAD = 7;
        final int iOCUPACION = 8;
        final int iNSS = 9;
        final int iCURP = 10;
        final int iRFC_TRABAJADOR = 11;
        final int iJORNADA = 12;

        // ===== Defaults base =====
        // if :NEW.ACTOR IS NULL THEN 99
        setIfNull(newRow, iACTOR, 99);

        // if :NEW.DEFENSA_ACT IS NULL THEN 9
        setIfNull(newRow, iDEFENSA_ACT, 9);

        Integer actor = asInt(newRow[iACTOR]);

        // ===== Actor: trabajador (actor = 1) =====
        if (actor != null && actor == 1) {

            setIfNull(newRow, iSEXO, 9);
            setIfNull(newRow, iEDAD, 99);
            setIfNull(newRow, iOCUPACION, 999);

            setIfNull(newRow, iNSS, "No Identificado");
            setIfNull(newRow, iCURP, "No Identificado");
            setIfNull(newRow, iRFC_TRABAJADOR, "No Identificado");

            setIfNull(newRow, iJORNADA, 9);
        }
    }

    @Override public void close() { }
    @Override public void remove() { }
}
