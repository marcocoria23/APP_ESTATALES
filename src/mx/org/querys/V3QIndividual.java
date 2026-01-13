package mx.org.querys;

import Conexion.ConexionH2;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class V3QIndividual {

    private static final Logger LOG = Logger.getLogger(V3QIndividual.class.getName());

    private String sql;
    private ArrayList<String[]> Array;

    // =========================
    // 1) Año judicial Campeche
    // =========================
    public ArrayList<String[]> Año_JudicialCampeche() {
        Array = new ArrayList<>();

        // NOTA: Aquí conservo tu lógica, pero en H2:
        // - YEAR(fecha) para sacar año
        // - concatenación con || para armar fechas
        // - DATE 'YYYY-MM-DD' para comparaciones si aplica
        sql =
            "SELECT clave_organo, expediente_clave, " +
            "       FORMATDATETIME(FECHA_APERTURA_EXPEDIENTE,'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTES " +
            "FROM (" +
            "   SELECT clave_organo, expediente_clave, FECHA_APERTURA_EXPEDIENTE, " +
            "          CAST(YEAR(FECHA_APERTURA_EXPEDIENTE) AS VARCHAR) AS FECHA_APERTURA_ANIO, " +
            "          CAST(YEAR(FECHA_APERTURA_EXPEDIENTE)+1 AS VARCHAR) AS FECHA_APERTURA_ANIO_SIG, " +
            "          SUBSTRING(REGEXP_REPLACE(expediente_clave, '[^0-9]', ''), LENGTH(REGEXP_REPLACE(expediente_clave, '[^0-9]', ''))-3, 4) AS EXPE_ANIO " +
            "   FROM V3_TR_INDIVIDUALJL" +
            ") x " +
            "WHERE x.FECHA_APERTURA_ANIO <> x.EXPE_ANIO " +
            "AND x.FECHA_APERTURA_EXPEDIENTE NOT BETWEEN " +
            "    PARSEDATETIME('01/09/' || x.FECHA_APERTURA_ANIO, 'dd/MM/yyyy') " +
            "    AND PARSEDATETIME('01/08/' || x.FECHA_APERTURA_ANIO_SIG, 'dd/MM/yyyy')";

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("clave_organo"),
                    rs.getString("expediente_clave"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTES")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Año_JudicialCampeche", ex);
        }

        return Array;
    }

    // ==========================================
    // 2) Año diferente Campeche (2020-2022 etc)
    // ==========================================
    public ArrayList<String[]> Año_DIF_Campeche() {
        Array = new ArrayList<>();

        // Aquí quité el filtro PValidacion.AñoJuridico.
        // Si quieres seguir excluyendo años, pon IN('2020','2021','2022'...)
        sql =
            "SELECT clave_organo, expediente_clave, " +
            "       FORMATDATETIME(FECHA_APERTURA_EXPEDIENTE,'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTES " +
            "FROM (" +
            "   SELECT clave_organo, expediente_clave, FECHA_APERTURA_EXPEDIENTE, " +
            "          CAST(YEAR(FECHA_APERTURA_EXPEDIENTE) AS VARCHAR) AS FECHA_APERTURA_ANIO, " +
            "          SUBSTRING(REGEXP_REPLACE(expediente_clave, '[^0-9]', ''), LENGTH(REGEXP_REPLACE(expediente_clave, '[^0-9]', ''))-3, 4) AS EXPE_ANIO " +
            "   FROM V3_TR_INDIVIDUALJL" +
            ") x " +
            "WHERE x.FECHA_APERTURA_ANIO <> x.EXPE_ANIO";

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("clave_organo"),
                    rs.getString("expediente_clave"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTES")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Año_DIF_Campeche", ex);
        }

        return Array;
    }

    // ======================================
    // 3) Año apertura vs año en expediente NE
    // ======================================
    public ArrayList<String[]> Año_Expe_IndividualNE() {
        Array = new ArrayList<>();

        sql =
            "SELECT clave_organo, expediente_clave, " +
            "       FORMATDATETIME(FECHA_APERTURA_EXPEDIENTE,'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTES, " +
            "       COMENTARIOS " +
            "FROM (" +
            "   SELECT clave_organo, expediente_clave, FECHA_APERTURA_EXPEDIENTE, " +
            "          CAST(YEAR(FECHA_APERTURA_EXPEDIENTE) AS VARCHAR) AS FECHA_APERTURA_ANIO, " +
            "          SUBSTRING(REGEXP_REPLACE(expediente_clave, '[^0-9]', ''), LENGTH(REGEXP_REPLACE(expediente_clave, '[^0-9]', ''))-3, 4) AS EXPE_ANIO, " +
            "          COMENTARIOS " +
            "   FROM V3_TR_INDIVIDUALJL" +
            ") x " +
            "WHERE x.FECHA_APERTURA_ANIO <> x.EXPE_ANIO " +
            "AND x.EXPE_ANIO NOT IN ('2020','2021','2022','2023','2024','2025')";

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("clave_organo"),
                    rs.getString("expediente_clave"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTES"),
                    rs.getString("COMENTARIOS")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Año_Expe_IndividualNE", ex);
        }

        return Array;
    }

    // ==========================
    // 4) Fechas futuras (varias)
    // ==========================
    public ArrayList<String[]> FECHA_APERTURA_EXPEDIENTE_FUT() {
        return fechaFutura("FECHA_APERTURA_EXPEDIENTE", "FECHA_APERTURA_EXPEDIENTE");
    }

    public ArrayList<String[]> FECHA_PRES_DEMANDA_FUT() {
        return fechaFutura("FECHA_PRES_DEMANDA", "FECHA_PRES_DEMANDA");
    }

    public ArrayList<String[]> FECHA_ADMI_DEMANDA_FUT() {
        return fechaFutura("FECHA_ADMI_DEMANDA", "FECHA_ADMI_DEMANDA");
    }

    public ArrayList<String[]> FECHA_DEPURACION_FUT() {
        return fechaFutura("FECHA_DEPURACION", "FECHA_DEPURACION");
    }

    public ArrayList<String[]> FECHA_AUDIENCIA_PRELIM_FUT() {
        return fechaFutura("FECHA_AUDIENCIA_PRELIM", "FECHA_AUDIENCIA_PRELIM");
    }

    public ArrayList<String[]> FECHA_AUDIENCIA_JUICIO_FUT() {
        return fechaFutura("FECHA_AUDIENCIA_JUICIO", "FECHA_AUDIENCIA_JUICIO");
    }

    public ArrayList<String[]> FECHA_ACTO_PROCESAL_FUT() {
        return fechaFutura("FECHA_ACTO_PROCESAL", "FECHA_ACTO_PROCESAL");
    }

    public ArrayList<String[]> FECHA_DICTO_RESOLUCION_AD_FUT() {
        return fechaFutura("FECHA_DICTO_RESOLUCION_AD", "FECHA_DICTO_RESOLUCION_AD");
    }

    public ArrayList<String[]> FECHA_RESOLUCION_TA_FUT() {
        return fechaFutura("FECHA_RESOLUCION_TA", "FECHA_RESOLUCION_TA");
    }

    public ArrayList<String[]> FECHA_DICTO_RESOLUCION_AP_FUT() {
        return fechaFutura("FECHA_DICTO_RESOLUCION_AP", "FECHA_DICTO_RESOLUCION_AP");
    }

    public ArrayList<String[]> FECHA_DICTO_RESOLUCION_AJ_FUT() {
        return fechaFutura("FECHA_DICTO_RESOLUCION_AJ", "FECHA_DICTO_RESOLUCION_AJ");
    }

    private ArrayList<String[]> fechaFutura(String campoFecha, String alias) {
        ArrayList<String[]> out = new ArrayList<>();

        // Si el campo es DATE/TIMESTAMP:
        // - Comparar directo > CURRENT_DATE
        // - Excluir sentinela 1899-09-09
        sql =
            "SELECT clave_organo, expediente_clave, periodo, " +
            "       FORMATDATETIME(" + campoFecha + ",'dd/MM/yyyy') AS " + alias + " " +
            "FROM V3_TR_INDIVIDUALJL " +
            "WHERE " + campoFecha + " > CURRENT_DATE " +
            "  AND " + campoFecha + " <> DATE '1899-09-09'";

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                out.add(new String[]{
                    rs.getString("clave_organo"),
                    rs.getString("expediente_clave"),
                    rs.getString("periodo"),
                    rs.getString(alias)
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en fechaFutura(" + campoFecha + ")", ex);
        }

        return out;
    }

    // ==========================
    // 5) Duplicidad de expediente
    // ==========================
    public ArrayList<String[]> Duplicidad_expediente() {
        Array = new ArrayList<>();

        sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(FECHA_APERTURA_EXPEDIENTE,'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE " +
            "FROM V3_TR_INDIVIDUALJL t " +
            "WHERE (t.CLAVE_ORGANO || REGEXP_REPLACE(t.EXPEDIENTE_CLAVE, '[^0-9]', '') || t.PERIODO) IN (" +
            "   SELECT (CLAVE_ORGANO || EXP2 || PERIODO) FROM (" +
            "       SELECT CLAVE_ORGANO, PERIODO, REGEXP_REPLACE(EXPEDIENTE_CLAVE, '[^0-9]', '') AS EXP2, COUNT(*) AS CUENTA " +
            "       FROM V3_TR_INDIVIDUALJL " +
            "       GROUP BY CLAVE_ORGANO, PERIODO, REGEXP_REPLACE(EXPEDIENTE_CLAVE, '[^0-9]', '') " +
            "   ) x WHERE x.CUENTA > 1" +
            ") " +
            "ORDER BY CLAVE_ORGANO, REGEXP_REPLACE(EXPEDIENTE_CLAVE, '[^0-9]', '')";

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Duplicidad_expediente", ex);
        }

        return Array;
    }

    // ==================================
    // 6) Presentación vs apertura (NE)
    // ==================================
    public ArrayList<String[]> Fecha_PresentacionNE() {
        Array = new ArrayList<>();

        // Si FECHA_APERTURA_EXPEDIENTE y FECHA_PRES_DEMANDA son DATE:
        // comparamos directo sin to_date()
        sql =
            "SELECT entidad_clave, clave_organo, expediente_clave, " +
            "       FORMATDATETIME(FECHA_APERTURA_EXPEDIENTE,'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(FECHA_PRES_DEMANDA,'dd/MM/yyyy') AS FECHA_PRES_DEMANDA " +
            "FROM V3_TR_INDIVIDUALJL " +
            "WHERE FECHA_APERTURA_EXPEDIENTE < FECHA_PRES_DEMANDA " +
            "  AND FECHA_PRES_DEMANDA <> DATE '1899-09-09'";

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("entidad_clave"),
                    rs.getString("clave_organo"),
                    rs.getString("expediente_clave"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE"),
                    rs.getString("FECHA_PRES_DEMANDA")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Fecha_PresentacionNE", ex);
        }

        return Array;
    }

    // ======================================================
    // 7) Admisión < Presentación (NE)  (sin filtros)
    // ======================================================
    public ArrayList<String[]> Fecha_PresentacionAdmiNE() {
        Array = new ArrayList<>();

        sql =
            "SELECT entidad_clave, clave_organo, expediente_clave, " +
            "       FORMATDATETIME(FECHA_ADMI_DEMANDA,'dd/MM/yyyy') AS FECHA_ADMI_DEMANDA, " +
            "       FORMATDATETIME(FECHA_PRES_DEMANDA,'dd/MM/yyyy') AS FECHA_PRES_DEMANDA " +
            "FROM V3_TR_INDIVIDUALJL " +
            "WHERE FECHA_ADMI_DEMANDA < FECHA_PRES_DEMANDA " +
            "  AND FECHA_PRES_DEMANDA <> DATE '1899-09-09'";

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("entidad_clave"),
                    rs.getString("clave_organo"),
                    rs.getString("expediente_clave"),
                    rs.getString("FECHA_ADMI_DEMANDA"),
                    rs.getString("FECHA_PRES_DEMANDA")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Fecha_PresentacionAdmiNE", ex);
        }

        return Array;
    }

    // ==========================================
    // 8) Admisión < Apertura (NE) (sin filtros)
    // ==========================================
    public ArrayList<String[]> Fecha_Admi_demandaNE() {
        Array = new ArrayList<>();

        sql =
            "SELECT entidad_clave, clave_organo, expediente_clave, " +
            "       FORMATDATETIME(FECHA_APERTURA_EXPEDIENTE,'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(FECHA_ADMI_DEMANDA,'dd/MM/yyyy') AS FECHA_ADMI_DEMANDA, " +
            "       COMENTARIOS " +
            "FROM V3_TR_INDIVIDUALJL " +
            "WHERE FECHA_ADMI_DEMANDA <> DATE '1899-09-09' " +
            "  AND FECHA_ADMI_DEMANDA < FECHA_APERTURA_EXPEDIENTE";

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("entidad_clave"),
                    rs.getString("clave_organo"),
                    rs.getString("expediente_clave"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE"),
                    rs.getString("FECHA_ADMI_DEMANDA"),
                    rs.getString("COMENTARIOS")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Fecha_Admi_demandaNE", ex);
        }

        return Array;
    }

    // ==========================================
    // 9) Depuración <= Apertura (NE) (sin filtros)
    // ==========================================
    public ArrayList<String[]> Fecha_DepuracionNE() {
        Array = new ArrayList<>();

        sql =
            "SELECT entidad_clave, clave_organo, expediente_clave, " +
            "       FORMATDATETIME(FECHA_APERTURA_EXPEDIENTE,'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(FECHA_DEPURACION,'dd/MM/yyyy') AS FECHA_DEPURACION " +
            "FROM V3_TR_INDIVIDUALJL " +
            "WHERE FECHA_DEPURACION <> DATE '1899-09-09' " +
            "  AND FECHA_DEPURACION <= FECHA_APERTURA_EXPEDIENTE";

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("entidad_clave"),
                    rs.getString("clave_organo"),
                    rs.getString("expediente_clave"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE"),
                    rs.getString("FECHA_DEPURACION")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Fecha_DepuracionNE", ex);
        }

        return Array;
    }

    // ==========================================
    // 10) Aud prelim <= Apertura (NE) (sin filtros)
    // ==========================================
    public ArrayList<String[]> Fecha_Audiencia_PrelimNE() {
        Array = new ArrayList<>();

        sql =
            "SELECT entidad_clave, clave_organo, expediente_clave, " +
            "       FORMATDATETIME(FECHA_APERTURA_EXPEDIENTE,'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(FECHA_AUDIENCIA_PRELIM,'dd/MM/yyyy') AS FECHA_AUDIENCIA_PRELIM " +
            "FROM V3_TR_INDIVIDUALJL " +
            "WHERE FECHA_AUDIENCIA_PRELIM <> DATE '1899-09-09' " +
            "  AND FECHA_AUDIENCIA_PRELIM <= FECHA_APERTURA_EXPEDIENTE";

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("entidad_clave"),
                    rs.getString("clave_organo"),
                    rs.getString("expediente_clave"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE"),
                    rs.getString("FECHA_AUDIENCIA_PRELIM")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Fecha_Audiencia_PrelimNE", ex);
        }

        return Array;
    }

    // ==========================================
    // 11) Aud juicio <= Apertura (NE) (sin filtros)
    // ==========================================
    public ArrayList<String[]> Fecha_Audiencia_JuicioNE() {
        Array = new ArrayList<>();

        sql =
            "SELECT entidad_clave, clave_organo, expediente_clave, " +
            "       FORMATDATETIME(FECHA_APERTURA_EXPEDIENTE,'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(FECHA_AUDIENCIA_JUICIO,'dd/MM/yyyy') AS FECHA_AUDIENCIA_JUICIO " +
            "FROM V3_TR_INDIVIDUALJL " +
            "WHERE FECHA_AUDIENCIA_JUICIO <> DATE '1899-09-09' " +
            "  AND FECHA_AUDIENCIA_JUICIO <= FECHA_APERTURA_EXPEDIENTE";

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("entidad_clave"),
                    rs.getString("clave_organo"),
                    rs.getString("expediente_clave"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE"),
                    rs.getString("FECHA_AUDIENCIA_JUICIO")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Fecha_Audiencia_JuicioNE", ex);
        }

        return Array;
    }

    // ==========================================
    // 12) Acto procesal < Apertura (NE) (sin filtros)
    // ==========================================
    public ArrayList<String[]> Fecha_Acto_procesalNE() {
        Array = new ArrayList<>();

        sql =
            "SELECT entidad_clave, clave_organo, expediente_clave, " +
            "       FORMATDATETIME(FECHA_APERTURA_EXPEDIENTE,'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(FECHA_ACTO_PROCESAL,'dd/MM/yyyy') AS FECHA_ACTO_PROCESAL " +
            "FROM V3_TR_INDIVIDUALJL " +
            "WHERE FECHA_ACTO_PROCESAL <> DATE '1899-09-09' " +
            "  AND FECHA_ACTO_PROCESAL < FECHA_APERTURA_EXPEDIENTE";

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("entidad_clave"),
                    rs.getString("clave_organo"),
                    rs.getString("expediente_clave"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE"),
                    rs.getString("FECHA_ACTO_PROCESAL")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Fecha_Acto_procesalNE", ex);
        }

        return Array;
    }

    // ==========================================
    // 13) Dicto resolución AD < Apertura (NE)
    // ==========================================
    public ArrayList<String[]> Fecha_Dicto_Resolucion_AdNE() {
        Array = new ArrayList<>();

        sql =
            "SELECT entidad_clave, clave_organo, expediente_clave, " +
            "       FORMATDATETIME(FECHA_APERTURA_EXPEDIENTE,'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(FECHA_DICTO_RESOLUCION_AD,'dd/MM/yyyy') AS FECHA_DICTO_RESOLUCION_AD " +
            "FROM V3_TR_INDIVIDUALJL " +
            "WHERE FECHA_DICTO_RESOLUCION_AD <> DATE '1899-09-09' " +
            "  AND FECHA_DICTO_RESOLUCION_AD < FECHA_APERTURA_EXPEDIENTE";

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("entidad_clave"),
                    rs.getString("clave_organo"),
                    rs.getString("expediente_clave"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE"),
                    rs.getString("FECHA_DICTO_RESOLUCION_AD")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Fecha_Dicto_Resolucion_AdNE", ex);
        }

        return Array;
    }

    // ==========================================
    // 14) Resolución TA < Apertura (NE)
    // ==========================================
    public ArrayList<String[]> Fecha_Resolucion_TaNE() {
        Array = new ArrayList<>();

        sql =
            "SELECT entidad_clave, clave_organo, expediente_clave, " +
            "       FORMATDATETIME(FECHA_APERTURA_EXPEDIENTE,'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(FECHA_RESOLUCION_TA,'dd/MM/yyyy') AS FECHA_RESOLUCION_TA " +
            "FROM V3_TR_INDIVIDUALJL " +
            "WHERE FECHA_RESOLUCION_TA <> DATE '1899-09-09' " +
            "  AND FECHA_RESOLUCION_TA < FECHA_APERTURA_EXPEDIENTE";

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("entidad_clave"),
                    rs.getString("clave_organo"),
                    rs.getString("expediente_clave"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE"),
                    rs.getString("FECHA_RESOLUCION_TA")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Fecha_Resolucion_TaNE", ex);
        }

        return Array;
    }

    // ==========================================
    // 15) Resolución AP < Apertura (NE)
    // ==========================================
    public ArrayList<String[]> Fecha_Resolucion_ApNE() {
        Array = new ArrayList<>();

        sql =
            "SELECT entidad_clave, clave_organo, expediente_clave, " +
            "       FORMATDATETIME(FECHA_APERTURA_EXPEDIENTE,'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(FECHA_DICTO_RESOLUCION_AP,'dd/MM/yyyy') AS FECHA_DICTO_RESOLUCION_AP " +
            "FROM V3_TR_INDIVIDUALJL " +
            "WHERE FECHA_DICTO_RESOLUCION_AP <> DATE '1899-09-09' " +
            "  AND FECHA_DICTO_RESOLUCION_AP < FECHA_APERTURA_EXPEDIENTE";

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("entidad_clave"),
                    rs.getString("clave_organo"),
                    rs.getString("expediente_clave"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE"),
                    rs.getString("FECHA_DICTO_RESOLUCION_AP")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Fecha_Resolucion_ApNE", ex);
        }

        return Array;
    }

    // ==========================================
    // 16) Resolución AJ < Apertura (NE)
    // ==========================================
    public ArrayList<String[]> Fecha_Resolucion_AjNE() {
        Array = new ArrayList<>();

        sql =
            "SELECT entidad_clave, clave_organo, expediente_clave, " +
            "       FORMATDATETIME(FECHA_APERTURA_EXPEDIENTE,'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(FECHA_DICTO_RESOLUCION_AJ,'dd/MM/yyyy') AS FECHA_DICTO_RESOLUCION_AJ " +
            "FROM V3_TR_INDIVIDUALJL " +
            "WHERE FECHA_DICTO_RESOLUCION_AJ <> DATE '1899-09-09' " +
            "  AND FECHA_DICTO_RESOLUCION_AJ < FECHA_APERTURA_EXPEDIENTE";

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("entidad_clave"),
                    rs.getString("clave_organo"),
                    rs.getString("expediente_clave"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE"),
                    rs.getString("FECHA_DICTO_RESOLUCION_AJ")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Fecha_Resolucion_AjNE", ex);
        }

        return Array;
    }

    // =========================================================
    // 17) Cruces con Audiencias (sin filtros, tipo_proced = 2)
    // =========================================================
    public ArrayList<String[]> Fecha_Aud_Presentacion() {
        Array = new ArrayList<>();

        sql =
            "SELECT S.ENTIDAD_CLAVE, P.CLAVE_ORGANO, P.EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(P.FECHA_AUDIEN_CELEBRADA,'dd/MM/yyyy') AS FECHA_AUDIEN_CELEBRADA, " +
            "       FORMATDATETIME(S.FECHA_PRES_DEMANDA,'dd/MM/yyyy') AS FECHA_PRES_DEMANDA, " +
            "       P.ID_AUDIENCIA " +
            "FROM V3_TR_AUDIENCIASJL P " +
            "JOIN V3_TR_INDIVIDUALJL S " +
            "  ON P.CLAVE_ORGANO = S.CLAVE_ORGANO " +
            " AND P.EXPEDIENTE_CLAVE = S.EXPEDIENTE_CLAVE " +
            " AND P.PERIODO = S.PERIODO " +
            "WHERE P.TIPO_PROCED = 2 " +
            "  AND P.FECHA_AUDIEN_CELEBRADA <> DATE '1899-09-09' " +
            "  AND S.FECHA_PRES_DEMANDA <> DATE '1899-09-09' " +
            "  AND P.FECHA_AUDIEN_CELEBRADA < S.FECHA_PRES_DEMANDA";

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("ENTIDAD_CLAVE"),
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_AUDIEN_CELEBRADA"),
                    rs.getString("FECHA_PRES_DEMANDA"),
                    rs.getString("ID_AUDIENCIA")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Fecha_Aud_Presentacion", ex);
        }

        return Array;
    }

    public ArrayList<String[]> Fecha_Aud_Apertura() {
        Array = new ArrayList<>();

        sql =
            "SELECT S.ENTIDAD_CLAVE, P.CLAVE_ORGANO, P.EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(P.FECHA_AUDIEN_CELEBRADA,'dd/MM/yyyy') AS FECHA_AUDIEN_CELEBRADA, " +
            "       FORMATDATETIME(S.FECHA_APERTURA_EXPEDIENTE,'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       P.ID_AUDIENCIA " +
            "FROM V3_TR_AUDIENCIASJL P " +
            "JOIN V3_TR_INDIVIDUALJL S " +
            "  ON P.CLAVE_ORGANO = S.CLAVE_ORGANO " +
            " AND P.EXPEDIENTE_CLAVE = S.EXPEDIENTE_CLAVE " +
            " AND P.PERIODO = S.PERIODO " +
            "WHERE P.TIPO_PROCED = 2 " +
            "  AND P.FECHA_AUDIEN_CELEBRADA <> DATE '1899-09-09' " +
            "  AND S.FECHA_APERTURA_EXPEDIENTE <> DATE '1899-09-09' " +
            "  AND P.FECHA_AUDIEN_CELEBRADA < S.FECHA_APERTURA_EXPEDIENTE";

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("ENTIDAD_CLAVE"),
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_AUDIEN_CELEBRADA"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE"),
                    rs.getString("ID_AUDIENCIA")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Fecha_Aud_Apertura", ex);
        }

        return Array;
    }

    public ArrayList<String[]> Fecha_Aud_Admision() {
        Array = new ArrayList<>();

        sql =
            "SELECT S.ENTIDAD_CLAVE, P.CLAVE_ORGANO, P.EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(P.FECHA_AUDIEN_CELEBRADA,'dd/MM/yyyy') AS FECHA_AUDIEN_CELEBRADA, " +
            "       FORMATDATETIME(S.FECHA_ADMI_DEMANDA,'dd/MM/yyyy') AS FECHA_ADMI_DEMANDA, " +
            "       P.ID_AUDIENCIA " +
            "FROM V3_TR_AUDIENCIASJL P " +
            "JOIN V3_TR_INDIVIDUALJL S " +
            "  ON P.CLAVE_ORGANO = S.CLAVE_ORGANO " +
            " AND P.EXPEDIENTE_CLAVE = S.EXPEDIENTE_CLAVE " +
            " AND P.PERIODO = S.PERIODO " +
            "WHERE P.TIPO_PROCED = 2 " +
            "  AND P.FECHA_AUDIEN_CELEBRADA <> DATE '1899-09-09' " +
            "  AND S.FECHA_ADMI_DEMANDA <> DATE '1899-09-09' " +
            "  AND P.FECHA_AUDIEN_CELEBRADA < S.FECHA_ADMI_DEMANDA";

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("ENTIDAD_CLAVE"),
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_AUDIEN_CELEBRADA"),
                    rs.getString("FECHA_ADMI_DEMANDA"),
                    rs.getString("ID_AUDIENCIA")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Fecha_Aud_Admision", ex);
        }

        return Array;
    }
}
