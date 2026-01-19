package triggers;

import org.h2.api.Trigger;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;

public class V3TrHuelgaJL implements Trigger {

    private static final Date D_1899 = Date.valueOf("1899-09-09");
    private static final Date D_1999 = Date.valueOf("1999-09-09");

    @Override
    public void init(Connection conn, String schemaName, String triggerName,
                     String tableName, boolean before, int type) {
        // no-op
    }

    private void dbg(String msg) {
        System.out.println("[V3TrHuelgaJL] " + msg);
    }

    private Object get(Object[] row, int idx, String name) {
        if (row == null) throw new IllegalArgumentException("newRow es null");
        if (idx < 0 || idx >= row.length) {
            throw new IllegalArgumentException(
                "Indice fuera de rango: " + name + " idx=" + idx + " row.length=" + row.length
            );
        }
        return row[idx];
    }

    private void setIfNullDbg(Object[] row, int idx, String name, Object value) {
        Object cur = get(row, idx, name); // valida rango
        if (cur == null) {
            dbg("SET " + name + "[" + idx + "] = " + value + " (" +
                (value == null ? "null" : value.getClass().getName()) + ")");
            row[idx] = value;
        }
    }

    private Integer asInt(Object v) {
        if (v == null) return null;
        if (v instanceof Number) return ((Number) v).intValue();
        String s = v.toString().trim();
        if (s.isEmpty()) return null;
        return Integer.valueOf(s);
    }

    // OJO: Date.valueOf SOLO acepta yyyy-MM-dd
    private Date asDate(Object v) {
        if (v == null) return null;
        if (v instanceof Date) return (Date) v;
        if (v instanceof java.util.Date) return new Date(((java.util.Date) v).getTime());

        String s = v.toString().trim();
        if (s.isEmpty() || s.equalsIgnoreCase("null")) return null;

        // dd/MM/yyyy (por si llega desde CSV)
        if (s.matches("\\d{2}/\\d{2}/\\d{4}")) {
            if (s.equals("09/09/1999")) return D_1999;
            if (s.equals("09/09/1899")) return D_1899;
            String yyyy = s.substring(6, 10);
            String mm = s.substring(3, 5);
            String dd = s.substring(0, 2);
            return Date.valueOf(yyyy + "-" + mm + "-" + dd);
        }

        // yyyy-MM-dd HH:mm:ss -> corta a fecha
        if (s.length() >= 10 && s.charAt(4) == '-' && s.charAt(7) == '-') {
            return Date.valueOf(s.substring(0, 10));
        }

        return Date.valueOf(s); // yyyy-MM-dd
    }

    private void replace1999With1899Dbg(Object[] row, int idxDate, String name) {
        Object raw = get(row, idxDate, name);
        try {
            Date d = asDate(raw);
            if (d != null && d.equals(D_1999)) {
                dbg("NORMALIZA " + name + "[" + idxDate + "] " + raw + " -> " + D_1899);
                row[idxDate] = D_1899;
            }
        } catch (Exception e) {
            dbg("ERROR convirtiendo fecha en " + name + "[" + idxDate + "], valor=" + raw +
                " (" + (raw == null ? "null" : raw.getClass().getName()) + ")");
            throw e;
        }
    }

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {
        try {
            dbg("fire() row.length=" + (newRow == null ? -1 : newRow.length));

            // ===== ÍNDICES =====
            final int iFECHA_APERTURA_EXPEDIENTE = 3;
            final int iTIPO_ASUNTO              = 4;
            final int iRAMA_INDUS_INVOLUCRAD    = 5;
            final int iSECTOR_RAMA              = 6;
            final int iSUBSECTOR_RAMA           = 7;

            final int iOTRO_MOTIVO              = 19;
            final int iESPECIFIQUE_MOTIVO       = 20;

            final int iINCOMPETENCIA            = 21;
            final int iTIPO_INCOMPETENCIA       = 22;
            final int iESPECIFIQUE_INCOMP       = 23;
            final int iFECHA_PRESENTA_PETIC     = 24;

            final int iEMPLAZAMIENTO_HUELGA     = 27;
            final int iFECHA_EMPLAZAMIENTO      = 28;
            final int iPREHUELGA                = 29;
            final int iAUDIENCIA_CONCILIACION   = 30;
            final int iFECHA_AUDIENCIA          = 31;

            final int iESTALLAMIENTO_HUELGA     = 32;

            final int iESTATUS_EXPEDIENTE       = 35;
            final int iFECHA_ACTO_PROCESAL      = 36;
            final int iFASE_SOLI_EXPEDIENTE     = 37;

            final int iFORMA_SOLUCION_EMPLAZ    = 38;
            final int iESPECIFI_FORMA_EMPLAZ    = 39;
            final int iFECHA_RESOLU_EMPLAZ      = 40;

            final int iFORMA_SOLUCION_HUELGA    = 43;
            final int iESPECIFI_FORMA_HUELGA    = 44;
            final int iFECHA_RESOLU_HUELGA      = 45;
            final int iTIPO_SENTENCIA           = 46;

            final int iFECHA_ESTALLAM_HUELGA    = 47;
            final int iFECHA_LEVANT_HUELGA      = 48;

            // ===== Defaults con debug + validación de índice =====
            setIfNullDbg(newRow, iTIPO_ASUNTO, "TIPO_ASUNTO", 9);
            setIfNullDbg(newRow, iRAMA_INDUS_INVOLUCRAD, "RAMA_INDUS_INVOLUCRAD", "No Identificado");
            setIfNullDbg(newRow, iSECTOR_RAMA, "SECTOR_RAMA", 99);
            setIfNullDbg(newRow, iSUBSECTOR_RAMA, "SUBSECTOR_RAMA", 99);

            // Otro motivo => especifica
            Integer otroMotivo = asInt(get(newRow, iOTRO_MOTIVO, "OTRO_MOTIVO"));
            if (otroMotivo != null && otroMotivo == 1) {
                setIfNullDbg(newRow, iESPECIFIQUE_MOTIVO, "ESPECIFIQUE_MOTIVO", "No Especifico");
            }

            // Incompetencia default
            setIfNullDbg(newRow, iINCOMPETENCIA, "INCOMPETENCIA", 9);

            Integer incompetencia = asInt(get(newRow, iINCOMPETENCIA, "INCOMPETENCIA"));
            Integer tipoIncomp = asInt(get(newRow, iTIPO_INCOMPETENCIA, "TIPO_INCOMPETENCIA"));

            if (incompetencia != null && incompetencia == 1) {
                setIfNullDbg(newRow, iTIPO_INCOMPETENCIA, "TIPO_INCOMPETENCIA", 9);
                tipoIncomp = asInt(get(newRow, iTIPO_INCOMPETENCIA, "TIPO_INCOMPETENCIA"));
                if (tipoIncomp != null && tipoIncomp == 4) {
                    setIfNullDbg(newRow, iESPECIFIQUE_INCOMP, "ESPECIFIQUE_INCOMP", "No identificado");
                }
            }

            if (incompetencia != null && incompetencia == 2) {
                setIfNullDbg(newRow, iFECHA_PRESENTA_PETIC, "FECHA_PRESENTA_PETIC", D_1899);

                setIfNullDbg(newRow, iEMPLAZAMIENTO_HUELGA, "EMPLAZAMIENTO_HUELGA", 9);
                Integer emplaz = asInt(get(newRow, iEMPLAZAMIENTO_HUELGA, "EMPLAZAMIENTO_HUELGA"));
                if (emplaz != null && emplaz == 1) {
                    setIfNullDbg(newRow, iFECHA_EMPLAZAMIENTO, "FECHA_EMPLAZAMIENTO", D_1899);
                }

                setIfNullDbg(newRow, iPREHUELGA, "PREHUELGA", 9);
                Integer prehuelga = asInt(get(newRow, iPREHUELGA, "PREHUELGA"));

                if (prehuelga != null && prehuelga == 1) {
                    setIfNullDbg(newRow, iAUDIENCIA_CONCILIACION, "AUDIENCIA_CONCILIACION", 9);
                }

                Integer audienciaConc = asInt(get(newRow, iAUDIENCIA_CONCILIACION, "AUDIENCIA_CONCILIACION"));
                if (audienciaConc != null && audienciaConc == 1) {
                    setIfNullDbg(newRow, iFECHA_AUDIENCIA, "FECHA_AUDIENCIA", D_1899);
                }

                setIfNullDbg(newRow, iESTALLAMIENTO_HUELGA, "ESTALLAMIENTO_HUELGA", 9);

                setIfNullDbg(newRow, iESTATUS_EXPEDIENTE, "ESTATUS_EXPEDIENTE", 9);
                Integer estatusExp = asInt(get(newRow, iESTATUS_EXPEDIENTE, "ESTATUS_EXPEDIENTE"));

                if (estatusExp != null && estatusExp == 2) {
                    setIfNullDbg(newRow, iFECHA_ACTO_PROCESAL, "FECHA_ACTO_PROCESAL", D_1899);
                }

                if (estatusExp != null && estatusExp == 1) {
                    setIfNullDbg(newRow, iFASE_SOLI_EXPEDIENTE, "FASE_SOLI_EXPEDIENTE", 99);
                    Integer fase = asInt(get(newRow, iFASE_SOLI_EXPEDIENTE, "FASE_SOLI_EXPEDIENTE"));

                    if (fase != null && fase == 5) {
                        setIfNullDbg(newRow, iFORMA_SOLUCION_EMPLAZ, "FORMA_SOLUCION_EMPLAZ", 9);
                        setIfNullDbg(newRow, iFECHA_RESOLU_EMPLAZ, "FECHA_RESOLU_EMPLAZ", D_1899);
                    }

                    if (fase != null && fase == 6) {
                        setIfNullDbg(newRow, iFORMA_SOLUCION_EMPLAZ, "FORMA_SOLUCION_EMPLAZ", 9);
                        setIfNullDbg(newRow, iFECHA_RESOLU_EMPLAZ, "FECHA_RESOLU_EMPLAZ", D_1899);

                        Integer formaEmplaz = asInt(get(newRow, iFORMA_SOLUCION_EMPLAZ, "FORMA_SOLUCION_EMPLAZ"));
                        if (formaEmplaz != null && formaEmplaz == 7) {
                            setIfNullDbg(newRow, iESPECIFI_FORMA_EMPLAZ, "ESPECIFI_FORMA_EMPLAZ", "No Especifico");
                        }
                    }

                    if (fase != null && fase == 7) {
                        setIfNullDbg(newRow, iFORMA_SOLUCION_HUELGA, "FORMA_SOLUCION_HUELGA", 9);
                        setIfNullDbg(newRow, iFECHA_RESOLU_HUELGA, "FECHA_RESOLU_HUELGA", D_1899);

                        Integer formaHuelga = asInt(get(newRow, iFORMA_SOLUCION_HUELGA, "FORMA_SOLUCION_HUELGA"));
                        Integer estall = asInt(get(newRow, iESTALLAMIENTO_HUELGA, "ESTALLAMIENTO_HUELGA"));

                        if (estall != null && estall == 1) {
                            setIfNullDbg(newRow, iFECHA_ESTALLAM_HUELGA, "FECHA_ESTALLAM_HUELGA", D_1899);
                        }

                        setIfNullDbg(newRow, iFECHA_LEVANT_HUELGA, "FECHA_LEVANT_HUELGA", D_1899);

                        if (formaHuelga != null && formaHuelga == 3) {
                            setIfNullDbg(newRow, iTIPO_SENTENCIA, "TIPO_SENTENCIA", 9);
                        }

                        if (formaHuelga != null && formaHuelga == 5) {
                            setIfNullDbg(newRow, iESPECIFI_FORMA_HUELGA, "ESPECIFI_FORMA_HUELGA", "No identificado");
                        }
                    }
                }
            }

            // ===== Normalización fechas (con debug) =====
            replace1999With1899Dbg(newRow, iFECHA_APERTURA_EXPEDIENTE, "FECHA_APERTURA_EXPEDIENTE");
            replace1999With1899Dbg(newRow, iFECHA_PRESENTA_PETIC, "FECHA_PRESENTA_PETIC");
            replace1999With1899Dbg(newRow, iFECHA_EMPLAZAMIENTO, "FECHA_EMPLAZAMIENTO");
            replace1999With1899Dbg(newRow, iFECHA_AUDIENCIA, "FECHA_AUDIENCIA");
            replace1999With1899Dbg(newRow, iFECHA_ACTO_PROCESAL, "FECHA_ACTO_PROCESAL");
            replace1999With1899Dbg(newRow, iFECHA_RESOLU_EMPLAZ, "FECHA_RESOLU_EMPLAZ");
            replace1999With1899Dbg(newRow, iFECHA_RESOLU_HUELGA, "FECHA_RESOLU_HUELGA");
            replace1999With1899Dbg(newRow, iFECHA_ESTALLAM_HUELGA, "FECHA_ESTALLAM_HUELGA");
            replace1999With1899Dbg(newRow, iFECHA_LEVANT_HUELGA, "FECHA_LEVANT_HUELGA");

        } catch (Exception e) {
            dbg("EXCEPCION en trigger: " + e.getClass().getName() + " - " + e.getMessage());
            e.printStackTrace(System.out); // <-- AQUI veras la linea exacta
            throw e; // <-- importante: no te comas el error
        }
    }

    @Override public void close() { }
    @Override public void remove() { }
}
