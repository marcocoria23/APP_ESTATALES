package mx.org.querys;

import Conexion.ConexionH2;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class V3QParaprocesal {

    private static final Logger LOG = Logger.getLogger(V3QParaprocesal.class.getName());

    private String sql;
    private ArrayList<String[]> Array;

    // =========================================================
    // 1) Año judicial Campeche (sin filtros)
    // =========================================================
    public ArrayList<String[]> Año_JudicialCampeche() {
        Array = new ArrayList<>();

        sql =
            "SELECT clave_organo, expediente_clave, " +
            "       FORMATDATETIME(FECHA_APERTURA_EXPEDIENTE,'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTES " +
            "FROM (" +
            "   SELECT clave_organo, expediente_clave, FECHA_APERTURA_EXPEDIENTE, " +
            "          CAST(YEAR(FECHA_APERTURA_EXPEDIENTE) AS VARCHAR) AS FECHA_APERTURA_ANIO, " +
            "          CAST(YEAR(FECHA_APERTURA_EXPEDIENTE)+1 AS VARCHAR) AS FECHA_APERTURA_ANIO_SIG, " +
            "          SUBSTRING(REGEXP_REPLACE(expediente_clave, '[^0-9]', ''), " +
            "                   LENGTH(REGEXP_REPLACE(expediente_clave, '[^0-9]', ''))-3, 4) AS EXPE_ANIO " +
            "   FROM V3_TR_PARAPROCESALJL" +
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

    // =========================================================
    // 2) Año diferente Campeche (sin filtros)
    // =========================================================
    public ArrayList<String[]> Año_DIF_Campeche() {
        Array = new ArrayList<>();

        // En Oracle usabas: EXPE_AÑO NOT IN PValidacion.AñoJuridico
        // Aquí queda SIN ese filtro, solo detecta "año apertura != año expediente"
        sql =
            "SELECT clave_organo, expediente_clave, " +
            "       FORMATDATETIME(FECHA_APERTURA_EXPEDIENTE,'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTES " +
            "FROM (" +
            "   SELECT clave_organo, expediente_clave, FECHA_APERTURA_EXPEDIENTE, " +
            "          CAST(YEAR(FECHA_APERTURA_EXPEDIENTE) AS VARCHAR) AS FECHA_APERTURA_ANIO, " +
            "          SUBSTRING(REGEXP_REPLACE(expediente_clave, '[^0-9]', ''), " +
            "                   LENGTH(REGEXP_REPLACE(expediente_clave, '[^0-9]', ''))-3, 4) AS EXPE_ANIO " +
            "   FROM V3_TR_PARAPROCESALJL" +
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

    // =========================================================
    // 3) Año apertura vs año expediente (sin filtros)
    // =========================================================
    public ArrayList<String[]> Año_Expe_ParaprocesalNE() {
        Array = new ArrayList<>();

        sql =
            "SELECT clave_organo, expediente_clave, " +
            "       FORMATDATETIME(FECHA_APERTURA_EXPEDIENTE,'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTES " +
            "FROM (" +
            "   SELECT clave_organo, expediente_clave, FECHA_APERTURA_EXPEDIENTE, " +
            "          CAST(YEAR(FECHA_APERTURA_EXPEDIENTE) AS VARCHAR) AS FECHA_APERTURA_ANIO, " +
            "          SUBSTRING(REGEXP_REPLACE(expediente_clave, '[^0-9]', ''), " +
            "                   LENGTH(REGEXP_REPLACE(expediente_clave, '[^0-9]', ''))-3, 4) AS EXPE_ANIO " +
            "   FROM V3_TR_PARAPROCESALJL" +
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
                    rs.getString("FECHA_APERTURA_EXPEDIENTES")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Año_Expe_ParaprocesalNE", ex);
        }

        return Array;
    }

    // =========================================================
    // 4) Fechas futuras (varias) - sin filtros
    // =========================================================
    public ArrayList<String[]> FECHA_APERTURA_EXPEDIENTE_FUT() {
        return fechaFuturaParaprocesal("FECHA_APERTURA_EXPEDIENTE", "FECHA_APERTURA_EXPEDIENTE");
    }

    public ArrayList<String[]> FECHA_PRESENTA_SOLI_FUT() {
        return fechaFuturaParaprocesal("FECHA_PRESENTA_SOLI", "FECHA_PRESENTA_SOLI");
    }

    public ArrayList<String[]> FECHA_ADMISION_SOLI_FUT() {
        return fechaFuturaParaprocesal("FECHA_ADMISION_SOLI", "FECHA_ADMISION_SOLI");
    }

    public ArrayList<String[]> FECHA_CONCLUSION_EXPE_FUT() {
        return fechaFuturaParaprocesal("FECHA_CONCLUSION_EXPE", "FECHA_CONCLUSION_EXPE");
    }

    private ArrayList<String[]> fechaFuturaParaprocesal(String campoFecha, String alias) {
        ArrayList<String[]> out = new ArrayList<>();

        sql =
            "SELECT clave_organo, expediente_clave, periodo, " +
            "       FORMATDATETIME(" + campoFecha + ",'dd/MM/yyyy') AS " + alias + " " +
            "FROM V3_TR_PARAPROCESALJL " +
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
            LOG.log(Level.SEVERE, "Error en fechaFuturaParaprocesal(" + campoFecha + ")", ex);
        }

        return out;
    }

    // =========================================================
    // 5) Duplicidad expediente (sin filtros)
    // =========================================================
    public ArrayList<String[]> Duplicidad_expediente() {
        Array = new ArrayList<>();

        sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(FECHA_APERTURA_EXPEDIENTE,'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE " +
            "FROM V3_TR_PARAPROCESALJL t " +
            "WHERE (t.CLAVE_ORGANO || REGEXP_REPLACE(t.EXPEDIENTE_CLAVE, '[^0-9]', '') || t.PERIODO) IN (" +
            "   SELECT (CLAVE_ORGANO || EXP2 || PERIODO) FROM (" +
            "       SELECT CLAVE_ORGANO, PERIODO, REGEXP_REPLACE(EXPEDIENTE_CLAVE, '[^0-9]', '') AS EXP2, COUNT(*) AS CUENTA " +
            "       FROM V3_TR_PARAPROCESALJL " +
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

    // =========================================================
    // 6) Apertura < Presenta Solicitud (NE) (sin filtros)
    // =========================================================
    public ArrayList<String[]> Fecha_PresentacionNE() {
        Array = new ArrayList<>();

        // En tu Oracle tenías: FECHA_APERTURA < FECHA_PRESENTA_SOLI
        // Eso es exactamente lo que validas (NE)
        sql =
            "SELECT ESTATUS_EXPEDIENTE, CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(FECHA_APERTURA_EXPEDIENTE,'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(FECHA_PRESENTA_SOLI,'dd/MM/yyyy') AS FECHA_PRESENTA_SOLI, " +
            "       PERIODO " +
            "FROM V3_TR_PARAPROCESALJL " +
            "WHERE FECHA_APERTURA_EXPEDIENTE < FECHA_PRESENTA_SOLI " +
            "  AND FECHA_PRESENTA_SOLI <> DATE '1899-09-09'";

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("ESTATUS_EXPEDIENTE"),
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE"),
                    rs.getString("FECHA_PRESENTA_SOLI")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Fecha_PresentacionNE", ex);
        }

        return Array;
    }

    // =========================================================
    // 7) Admisión < Presenta Solicitud (NE) (sin filtros)
    // =========================================================
    public ArrayList<String[]> Fecha_PresentacionAdmiNE() {
        Array = new ArrayList<>();

        sql =
            "SELECT ESTATUS_EXPEDIENTE, CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(FECHA_ADMISION_SOLI,'dd/MM/yyyy') AS FECHA_ADMISION_SOLI, " +
            "       FORMATDATETIME(FECHA_PRESENTA_SOLI,'dd/MM/yyyy') AS FECHA_PRESENTA_SOLI, " +
            "       PERIODO " +
            "FROM V3_TR_PARAPROCESALJL " +
            "WHERE FECHA_ADMISION_SOLI < FECHA_PRESENTA_SOLI " +
            "  AND FECHA_PRESENTA_SOLI <> DATE '1899-09-09' " +
            "  AND FECHA_ADMISION_SOLI <> DATE '1899-09-09'";

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("ESTATUS_EXPEDIENTE"),
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_ADMISION_SOLI"),
                    rs.getString("FECHA_PRESENTA_SOLI")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Fecha_PresentacionAdmiNE", ex);
        }

        return Array;
    }

    // =========================================================
    // 8) Admisión < Apertura (NE) (sin filtros)
    // =========================================================
    public ArrayList<String[]> Fecha_Admision_SoliNE() {
        Array = new ArrayList<>();

        sql =
            "SELECT ESTATUS_EXPEDIENTE, CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(FECHA_APERTURA_EXPEDIENTE,'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(FECHA_ADMISION_SOLI,'dd/MM/yyyy') AS FECHA_ADMISION_SOLI, " +
            "       PERIODO " +
            "FROM V3_TR_PARAPROCESALJL " +
            "WHERE FECHA_ADMISION_SOLI <> DATE '1899-09-09' " +
            "  AND FECHA_ADMISION_SOLI < FECHA_APERTURA_EXPEDIENTE";

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("ESTATUS_EXPEDIENTE"),
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE"),
                    rs.getString("FECHA_ADMISION_SOLI")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Fecha_Admision_SoliNE", ex);
        }

        return Array;
    }

    // =========================================================
    // 9) Conclusión < Apertura (NE) (sin filtros)
    // =========================================================
    public ArrayList<String[]> Fecha_Conclusion_ExpeNE() {
        Array = new ArrayList<>();

        sql =
            "SELECT ESTATUS_EXPEDIENTE, CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(FECHA_APERTURA_EXPEDIENTE,'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(FECHA_CONCLUSION_EXPE,'dd/MM/yyyy') AS FECHA_CONCLUSION_EXPE, " +
            "       comentarios " +
            "FROM V3_TR_PARAPROCESALJL " +
            "WHERE FECHA_CONCLUSION_EXPE <> DATE '1899-09-09' " +
            "  AND FECHA_CONCLUSION_EXPE < FECHA_APERTURA_EXPEDIENTE";

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("ESTATUS_EXPEDIENTE"),
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE"),
                    rs.getString("FECHA_CONCLUSION_EXPE"),
                    rs.getString("comentarios")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Fecha_Conclusion_ExpeNE", ex);
        }

        return Array;
    }
}
