package triggers;

import org.h2.api.Trigger;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;

public class V3TrHuelgaJL implements Trigger {

    private static final LocalDate D_1899 = LocalDate.of(1899, 9, 9);
    private static final LocalDate D_1999 = LocalDate.of(1999, 9, 9);
    private static final String Sql_Error = "INSERT INTO ERRORES_INSERT "
            + "(TABLA_DESTINO, CLAVE_ORGANO, EXPEDIENTE_CLAVE, ID, SQLSTATE, ERRORCODE, MENSAJE, REGISTRO_RAW) "
            + "VALUES (?,?,?,?,?,?,?,?)";

    @Override
    public void init(Connection conn, String schemaName, String triggerName,
            String tableName, boolean before, int type) {
        // no-op
    }

    private String asString(Object v) {
        if (v == null) {
            return null;
        }
        String s = v.toString().trim();
        return (s.isEmpty() || "null".equalsIgnoreCase(s)) ? null : s;
    }

    private void dbg(String msg) {
        System.out.println("[V3TrHuelgaJL] " + msg);
    }

    private Object get(Object[] row, int idx, String name) {
        if (row == null) {
            throw new IllegalArgumentException("newRow es null");
        }
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
            dbg("SET " + name + "[" + idx + "] = " + value + " ("
                    + (value == null ? "null" : value.getClass().getName()) + ")");
            row[idx] = value;
        }
    }

    private Integer asInt(Object v) {
        if (v == null) {
            return null;
        }
        if (v instanceof Number) {
            return ((Number) v).intValue();
        }
        String s = v.toString().trim();
        if (s.isEmpty()) {
            return null;
        }
        return Integer.valueOf(s);
    }

    // OJO: Date.valueOf SOLO acepta yyyy-MM-dd
   private Date asDate(Object v) {
    if (v == null) return null;


    // Si ya viene como java.sql.Date, úsalo tal cual (pero ojo: si el origen ya viene mal, aquí no lo arreglas)
    if (v instanceof Date) return (Date) v;


    // Si viene java.util.Date (incluye Timestamp), conviértelo a LocalDate para evitar TZ en el formateo final
    if (v instanceof java.util.Date) {
        java.util.Date ud = (java.util.Date) v;
        // Convertir a LocalDate en zona UTC para evitar que cambie el día entre PCs
        LocalDate ld = ud.toInstant().atZone(java.time.ZoneOffset.UTC).toLocalDate();
        return Date.valueOf(ld);
    }


    String s = v.toString().trim();
    if (s.isEmpty() || s.equalsIgnoreCase("null")) return null;


    LocalDate ld;


    // dd/MM/yyyy (CSV)
    if (s.matches("\\d{2}/\\d{2}/\\d{4}")) {
        if (s.equals("09/09/1999")) ld = D_1999;
        else if (s.equals("09/09/1899")) ld = D_1899;
        else {
            int dd = Integer.parseInt(s.substring(0, 2));
            int mm = Integer.parseInt(s.substring(3, 5));
            int yyyy = Integer.parseInt(s.substring(6, 10));
            ld = LocalDate.of(yyyy, mm, dd);
        }
        return Date.valueOf(ld);
    }


    // yyyy-MM-dd HH:mm:ss (o yyyy-MM-ddTHH:mm:ss) -> solo fecha
    if (s.length() >= 10 && s.charAt(4) == '-' && s.charAt(7) == '-') {
        ld = LocalDate.parse(s.substring(0, 10)); // yyyy-MM-dd
        return Date.valueOf(ld);
    }


    // yyyy-MM-dd
    ld = LocalDate.parse(s);
    return Date.valueOf(ld);
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
            dbg("ERROR convirtiendo fecha en " + name + "[" + idxDate + "], valor=" + raw
                    + " (" + (raw == null ? "null" : raw.getClass().getName()) + ")");
            throw e;
        }
    }

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {
        try {
            dbg("fire() row.length=" + (newRow == null ? -1 : newRow.length));

            // ===== ÍNDICES =====
            final int iCLAVE_ORGANO = 1;
            final int iEXPEDIENTE_CLAVE = 2;
            final int iFECHA_APERTURA_EXPEDIENTE = 3;
            final int iTIPO_ASUNTO = 4;
            final int iRAMA_INDUS_INVOLUCRAD = 5;
            final int iSECTOR_RAMA = 6;
            final int iSUBSECTOR_RAMA = 7;

            final int iOTRO_MOTIVO = 19;
            final int iESPECIFIQUE_MOTIVO = 20;

            final int iINCOMPETENCIA = 21;
            final int iTIPO_INCOMPETENCIA = 22;
            final int iESPECIFIQUE_INCOMP = 23;
            final int iFECHA_PRESENTA_PETIC = 24;

            final int iEMPLAZAMIENTO_HUELGA = 27;
            final int iFECHA_EMPLAZAMIENTO = 28;
            final int iPREHUELGA = 29;
            final int iAUDIENCIA_CONCILIACION = 30;
            final int iFECHA_AUDIENCIA = 31;

            final int iESTALLAMIENTO_HUELGA = 32;

            final int iESTATUS_EXPEDIENTE = 35;
            final int iFECHA_ACTO_PROCESAL = 36;
            final int iFASE_SOLI_EXPEDIENTE = 37;

            final int iFORMA_SOLUCION_EMPLAZ = 38;
            final int iESPECIFI_FORMA_EMPLAZ = 39;
            final int iFECHA_RESOLU_EMPLAZ = 40;

            final int iFORMA_SOLUCION_HUELGA = 43;
            final int iESPECIFI_FORMA_HUELGA = 44;
            final int iFECHA_RESOLU_HUELGA = 45;
            final int iTIPO_SENTENCIA = 46;

            final int iFECHA_ESTALLAM_HUELGA = 47;
            final int iFECHA_LEVANT_HUELGA = 48;

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

            Integer TipoAsunto = asInt(get(newRow, iTIPO_ASUNTO, "TIPO_ASUNTO"));
            if (TipoAsunto == 1) {
                String claveOrgano = asString(newRow[iCLAVE_ORGANO]);
                String expediente = asString(newRow[iEXPEDIENTE_CLAVE]);
                try (PreparedStatement pe = conn.prepareStatement(Sql_Error)) {
                    pe.setString(1, "V3_TR_HUELGA");
                    pe.setString(2, claveOrgano);
                    pe.setString(3, expediente);
                    pe.setString(4, "");
                    pe.setString(5, "");
                    pe.setInt(6, 0);
                    pe.setString(7, "El campo 'Tipo de Asunto' solo puede tener el valor= 2.-Colectivo");
                    pe.setString(8, "");
                    pe.executeUpdate();
                } catch (SQLException ex) {
                    // Si hasta la tabla de errores falla, al menos lo imprimimos
                    System.err.println("❌ No se pudo guardar en ERRORES_INSERT: " + ex.getMessage());
                }
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
            
            Integer fase = asInt(newRow[iFASE_SOLI_EXPEDIENTE]);
             if (fase != null
                            && fase != 5 && fase != 6 && fase != 7 && fase != 99) {
                        String claveOrgano = asString(newRow[iCLAVE_ORGANO]);
                        String expediente = asString(newRow[iEXPEDIENTE_CLAVE]);
                        try ( PreparedStatement pe = conn.prepareStatement(Sql_Error)) {
                            pe.setString(1, "V3_TR_HUELGAJL");
                        pe.setString(2, claveOrgano);
                        pe.setString(3, expediente);
                        pe.setString(4, "");
                        pe.setString(5, "");
                        pe.setInt(6, 0);
                        pe.setString(7, "El campo 'Fase en la que se solucionó el expediente' solo puede tener el valor= 5.-Emplazamiento a huelga, 6.-Prehuelga o 7.-Huelga");
                        pe.setString(8, "");
                        pe.executeUpdate();
                    } catch (SQLException ex) {
                        // Si hasta la tabla de errores falla, al menos lo imprimimos
                        System.err.println("❌ No se pudo guardar en ERRORES_INSERT: " + ex.getMessage());
                    }
                }

        } catch (Exception e) {
            dbg("EXCEPCION en trigger: " + e.getClass().getName() + " - " + e.getMessage());
            e.printStackTrace(System.out); // <-- AQUI veras la linea exacta
            throw e; // <-- importante: no te comas el error
        }
    }

    @Override
    public void close() {
    }

    @Override
    public void remove() {
    }
}
