package triggers;

import org.h2.api.Trigger;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;

public class V3TrColectEconomJL implements Trigger {

    private static final Date D_1899 = Date.valueOf("1899-09-09");
    private static final Date D_1999 = Date.valueOf("1999-09-09");

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

    private Date asDate(Object v) {
        if (v == null) return null;
        if (v instanceof Date) return (Date) v;
        // Por si llega como java.util.Date:
        if (v instanceof java.util.Date) return new Date(((java.util.Date) v).getTime());
        // Último recurso: intentar parse ISO yyyy-mm-dd
        String s = v.toString().trim();
        if (s.isEmpty()) return null;
        return Date.valueOf(s);
    }

    private void replace1999With1899(Object[] newRow, int idxDate) {
        Date d = asDate(newRow[idxDate]);
        if (d != null && d.equals(D_1999)) newRow[idxDate] = D_1899;
    }

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {

        // ===== Índices (0-based) según tu CREATE TABLE =====
        final int iFECHA_APERTURA_EXPEDIENTE = 3;
        final int iTIPO_ASUNTO = 4;
        final int iNAT_CONFLICTO = 5;
        final int iRAMA_INVOLUCRAD = 6;
        final int iSECTOR_RAMA = 7;
        final int iSUBSECTOR_RAMA = 8;
        final int iMODIF_CONDICIONES = 13;
        final int iNUEVAS_CONDICIONES = 14;
        final int iSUSPENSION_TEMPORAL = 15;
        final int iTERMINACION_COLECTIVA = 16;
        final int iOTRO_MOTIVO_ECONOM = 17;
        final int iESPECIFIQUE_ECONOM = 18;
        final int iEXCESO_PRODUCCION = 19;
        final int iINCOSTEABILIDAD = 20;
        final int iFALTA_FONDOS = 21;
        final int iINCOMPETENCIA = 22;
        final int iTIPO_INCOMPETENCIA = 23;
        final int iESPECIFIQUE_INCOMP = 24; // no lo usas en trigger Oracle
        final int iFECHA_PRES_DEMANDA = 25;
        final int iCONSTANCIA_CONS_EXPEDIDA = 26;
        final int iCONSTANCIA_CLAVE = 27;
        final int iASUN_EXCEP_CONCILIACION = 28;
        final int iPREVE_DEMANDA = 29;
        final int iDESAHOGO_PREV_DEMANDA = 30;
        final int iESTATUS_DEMANDA = 31;
        final int iFECHA_ADMISION_DEMANDA = 32;
        final int iAUDIENCIA_ECONOM = 35;
        final int iFECHA_AUDIENCIA_ECONOM = 36;
        final int iESTATUS_EXPEDIENTE = 37;
        final int iFECHA_ACTO_PROCESAL = 38;
        final int iFASE_SOLI_EXPEDIENTE = 39;
        final int iFORMA_SOLUCION = 40;
        final int iESPECIFIQUE_FORMA = 41; // no lo usas en trigger Oracle
        final int iFECHA_RESOLUCION = 42;
        final int iTIPO_SENTENCIA = 43;
        final int iAUMENTO_PERSONAL = 44;
        final int iDISMINUCION_PERSONAL = 45;
        final int iAUMENTO_JORNADALAB = 46;
        final int iDISMINUCION_JORNADALAB = 47;
        final int iAUMENTO_SEMANA = 48;
        final int iDISMINUCION_SEMANA = 49;
        final int iAUMENTO_SALARIOS = 50;
        final int iDISMINUCION_SALARIOS = 51;
        final int iOTRO_TIPO = 52;
        final int iESPECIFIQUE_TIPO = 53;

        // ===== Defaults base (tal cual Oracle) =====
        setIfNull(newRow, iTIPO_ASUNTO, 9);
        setIfNull(newRow, iNAT_CONFLICTO, 9);

        setIfNull(newRow, iRAMA_INVOLUCRAD, "No Identificado");
        setIfNull(newRow, iSECTOR_RAMA, 99);
        setIfNull(newRow, iSUBSECTOR_RAMA, 99);

        setIfNull(newRow, iMODIF_CONDICIONES, 9);
        setIfNull(newRow, iNUEVAS_CONDICIONES, 9);
        setIfNull(newRow, iSUSPENSION_TEMPORAL, 9);
        setIfNull(newRow, iTERMINACION_COLECTIVA, 9);
        setIfNull(newRow, iOTRO_MOTIVO_ECONOM, 9);

        Integer otroMotivo = asInt(newRow[iOTRO_MOTIVO_ECONOM]);
        if (otroMotivo != null && otroMotivo == 1) {
            setIfNull(newRow, iESPECIFIQUE_ECONOM, "No Especifico");
        }

        Integer suspension = asInt(newRow[iSUSPENSION_TEMPORAL]);
        if (suspension != null && suspension == 1) {
            setIfNull(newRow, iEXCESO_PRODUCCION, 9);
            setIfNull(newRow, iINCOSTEABILIDAD, 9);
            setIfNull(newRow, iFALTA_FONDOS, 9);
        }

        Integer incompetencia = asInt(newRow[iINCOMPETENCIA]);
        if (incompetencia != null && incompetencia == 1) {
            setIfNull(newRow, iTIPO_INCOMPETENCIA, 9);
        }

        if (incompetencia != null && incompetencia == 2) {
            setIfNull(newRow, iFECHA_PRES_DEMANDA, D_1899);
            setIfNull(newRow, iCONSTANCIA_CONS_EXPEDIDA, 9);

            Integer constExp = asInt(newRow[iCONSTANCIA_CONS_EXPEDIDA]);
            if (constExp != null && constExp == 1) {
                setIfNull(newRow, iCONSTANCIA_CLAVE, "No Identificado");
            }
            if (constExp != null && constExp == 2) {
                setIfNull(newRow, iASUN_EXCEP_CONCILIACION, 9);
            }

            setIfNull(newRow, iPREVE_DEMANDA, 9);

            Integer preve = asInt(newRow[iPREVE_DEMANDA]);
            if (preve != null && preve == 1) {
                setIfNull(newRow, iDESAHOGO_PREV_DEMANDA, 9);
            }

            setIfNull(newRow, iESTATUS_DEMANDA, 9);

            Integer estatusDem = asInt(newRow[iESTATUS_DEMANDA]);
            if (estatusDem != null && estatusDem == 1) {
                setIfNull(newRow, iFECHA_ADMISION_DEMANDA, D_1899);
                setIfNull(newRow, iAUDIENCIA_ECONOM, 9);

                Integer audEconom = asInt(newRow[iAUDIENCIA_ECONOM]);
                if (audEconom != null && audEconom == 1) {
                    setIfNull(newRow, iFECHA_AUDIENCIA_ECONOM, D_1899);
                }

                setIfNull(newRow, iESTATUS_EXPEDIENTE, 9);

                Integer estatusExp = asInt(newRow[iESTATUS_EXPEDIENTE]);
                if (estatusExp != null && estatusExp == 1) {
                    setIfNull(newRow, iFASE_SOLI_EXPEDIENTE, 99);

                    Integer fase = asInt(newRow[iFASE_SOLI_EXPEDIENTE]);
                    if (fase != null && fase == 8) {
                        setIfNull(newRow, iFORMA_SOLUCION, 9);
                        setIfNull(newRow, iFECHA_RESOLUCION, D_1899);

                        Integer forma = asInt(newRow[iFORMA_SOLUCION]);
                        if (forma != null && forma == 1) {
                            setIfNull(newRow, iTIPO_SENTENCIA, 9);
                            setIfNull(newRow, iAUMENTO_PERSONAL, 9);
                            setIfNull(newRow, iDISMINUCION_PERSONAL, 9);
                            setIfNull(newRow, iAUMENTO_JORNADALAB, 9);
                            setIfNull(newRow, iDISMINUCION_JORNADALAB, 9);
                            setIfNull(newRow, iAUMENTO_SEMANA, 9);
                            setIfNull(newRow, iDISMINUCION_SEMANA, 9);
                            setIfNull(newRow, iAUMENTO_SALARIOS, 9);
                            setIfNull(newRow, iDISMINUCION_SALARIOS, 9);
                            setIfNull(newRow, iOTRO_TIPO, 9);

                            Integer otroTipo = asInt(newRow[iOTRO_TIPO]);
                            if (otroTipo != null && otroTipo == 1) {
                                setIfNull(newRow, iESPECIFIQUE_TIPO, "No especifico");
                            }
                        }
                    }
                }

                if (estatusExp != null && estatusExp == 2) {
                    setIfNull(newRow, iFECHA_ACTO_PROCESAL, D_1899);
                }
            }
        }

        // ===== Normalización 09/09/1999 -> 1899-09-09 =====
        replace1999With1899(newRow, iFECHA_APERTURA_EXPEDIENTE);
        replace1999With1899(newRow, iFECHA_PRES_DEMANDA);
        replace1999With1899(newRow, iFECHA_ADMISION_DEMANDA);
        replace1999With1899(newRow, iFECHA_AUDIENCIA_ECONOM);
        replace1999With1899(newRow, iFECHA_ACTO_PROCESAL);
        replace1999With1899(newRow, iFECHA_RESOLUCION);
    }

    @Override public void close() { }
    @Override public void remove() { }
}
