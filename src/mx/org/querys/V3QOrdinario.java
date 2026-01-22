package mx.org.querys;

import Conexion.ConexionH2;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class V3QOrdinario {

    private static final Logger LOG = Logger.getLogger(V3QOrdinario.class.getName());

    private String sql;
    private ArrayList<String[]> Array;

    // =========================================================
    // 1) Año judicial Campeche (sin filtros)
    // =========================================================
    public ArrayList<String[]> Año_JudicialCampeche(Connection con) {
        Array = new ArrayList<>();

        sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(FECHA_APERTURA_EXPEDIENTE,'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTES " +
            "FROM (" +
            "   SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, FECHA_APERTURA_EXPEDIENTE, " +
            "          CAST(YEAR(FECHA_APERTURA_EXPEDIENTE) AS VARCHAR) AS FECHA_APERTURA_ANIO, " +
            "          CAST(YEAR(FECHA_APERTURA_EXPEDIENTE)+1 AS VARCHAR) AS FECHA_APERTURA_ANIO_SIG, " +
            "          SUBSTRING(REGEXP_REPLACE(EXPEDIENTE_CLAVE, '[^0-9]', ''), LENGTH(REGEXP_REPLACE(EXPEDIENTE_CLAVE, '[^0-9]', ''))-3, 4) AS EXPE_ANIO " +
            "   FROM V3_TR_ORDINARIOJL" +
            ") x " +
            "WHERE x.FECHA_APERTURA_ANIO <> x.EXPE_ANIO " +
            "AND x.FECHA_APERTURA_EXPEDIENTE NOT BETWEEN " +
            "    PARSEDATETIME('01/09/' || x.FECHA_APERTURA_ANIO, 'dd/MM/yyyy') " +
            "    AND PARSEDATETIME('01/08/' || x.FECHA_APERTURA_ANIO_SIG, 'dd/MM/yyyy')";

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
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
    public ArrayList<String[]> Año_DIF_Campeche(Connection con) {
        Array = new ArrayList<>();

        // Antes: EXPE_AÑO NOT IN PValidacion.AñoJuridico
        // Aquí lo dejo sin ese filtro (solo detecta año diferente)
        sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(FECHA_APERTURA_EXPEDIENTE,'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTES " +
            "FROM (" +
            "   SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, FECHA_APERTURA_EXPEDIENTE, " +
            "          CAST(YEAR(FECHA_APERTURA_EXPEDIENTE) AS VARCHAR) AS FECHA_APERTURA_ANIO, " +
            "          SUBSTRING(REGEXP_REPLACE(EXPEDIENTE_CLAVE, '[^0-9]', ''), LENGTH(REGEXP_REPLACE(EXPEDIENTE_CLAVE, '[^0-9]', ''))-3, 4) AS EXPE_ANIO " +
            "   FROM V3_TR_ORDINARIOJL" +
            ") x " +
            "WHERE x.FECHA_APERTURA_ANIO <> x.EXPE_ANIO";

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
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
    public ArrayList<String[]> Año_Expe_OrdinarioNE(Connection con) {
        Array = new ArrayList<>();

        sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(FECHA_APERTURA_EXPEDIENTE,'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTES " +
            "FROM (" +
            "   SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, FECHA_APERTURA_EXPEDIENTE, " +
            "          CAST(YEAR(FECHA_APERTURA_EXPEDIENTE) AS VARCHAR) AS FECHA_APERTURA_ANIO, " +
            "          SUBSTRING(REGEXP_REPLACE(EXPEDIENTE_CLAVE, '[^0-9]', ''), LENGTH(REGEXP_REPLACE(EXPEDIENTE_CLAVE, '[^0-9]', ''))-3, 4) AS EXPE_ANIO " +
            "   FROM V3_TR_ORDINARIOJL" +
            ") x " +
            "WHERE x.FECHA_APERTURA_ANIO <> x.EXPE_ANIO " +
            "AND x.EXPE_ANIO NOT IN ('2020','2021','2022','2023','2024','2025')";

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTES")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Año_Expe_OrdinarioNE", ex);
        }

        return Array;
    }

    // =========================================================
    // 4) Fechas futuras (varias) - sin filtros
    // =========================================================
    public ArrayList<String[]> FECHA_APERTURA_EXPEDIENTE_FUT(Connection con) {
        return fechaFuturaOrdinario("FECHA_APERTURA_EXPEDIENTE", "FECHA_APERTURA_EXPEDIENTE", con);
    }

    public ArrayList<String[]> FECHA_PRES_DEMANDA_FUT(Connection con) {
        return fechaFuturaOrdinario("FECHA_PRES_DEMANDA", "FECHA_PRES_DEMANDA", con);
    }

    public ArrayList<String[]> FECHA_ADMI_DEMANDA_FUT(Connection con) {
        return fechaFuturaOrdinario("FECHA_ADMI_DEMANDA", "FECHA_ADMI_DEMANDA", con);
    }

    public ArrayList<String[]> FECHA_AUDIENCIA_PRELIM_FUT(Connection con) {
        return fechaFuturaOrdinario("FECHA_AUDIENCIA_PRELIM", "FECHA_AUDIENCIA_PRELIM", con);
    }

    public ArrayList<String[]> FECHA_AUDIENCIA_JUICIO_FUT(Connection con) {
        return fechaFuturaOrdinario("FECHA_AUDIENCIA_JUICIO", "FECHA_AUDIENCIA_JUICIO", con);
    }

    public ArrayList<String[]> FECHA_ACTO_PROCESAL_FUT(Connection con) {
        return fechaFuturaOrdinario("FECHA_ACTO_PROCESAL", "FECHA_ACTO_PROCESAL", con);
    }

    public ArrayList<String[]> FECHA_DICTO_RESOLUCIONFE_FUT(Connection con) {
        return fechaFuturaOrdinario("FECHA_DICTO_RESOLUCIONFE", "FECHA_DICTO_RESOLUCIONFE", con);
    }

    public ArrayList<String[]> FECHA_DICTO_RESOLUCIONAP_FUT(Connection con) {
        return fechaFuturaOrdinario("FECHA_DICTO_RESOLUCIONAP", "FECHA_DICTO_RESOLUCIONAP", con);
    }

    public ArrayList<String[]> FECHA_RESOLUCIONAJ_FUT(Connection con) {
        return fechaFuturaOrdinario("FECHA_RESOLUCIONAJ", "FECHA_RESOLUCIONAJ", con);
    }

    private ArrayList<String[]> fechaFuturaOrdinario(String campoFecha, String alias, Connection con) {
        ArrayList<String[]> out = new ArrayList<>();

        sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE,  " +
            "       FORMATDATETIME(" + campoFecha + ",'dd/MM/yyyy') AS " + alias + " " +
            "FROM V3_TR_ORDINARIOJL " +
            "WHERE " + campoFecha + " > CURRENT_DATE " +
            "  AND " + campoFecha + " <> DATE '1899-09-09'";

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                out.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    //NOTA: MODIFICAR CLASE PRINCIPAL
                    rs.getString(alias)
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en fechaFuturaOrdinario(" + campoFecha + ")", ex);
        }

        return out;
    }

    // =========================================================
    // 5) Duplicidad expediente (sin filtros)
    // =========================================================
    public ArrayList<String[]> Duplicidad_expediente(Connection con) {
        Array = new ArrayList<>();

        sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(FECHA_APERTURA_EXPEDIENTE,'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE " +
            "FROM V3_TR_ORDINARIOJL t " +
            "WHERE (t.CLAVE_ORGANO || REGEXP_REPLACE(t.EXPEDIENTE_CLAVE, '[^0-9]', '') ) IN (" +
            "   SELECT (CLAVE_ORGANO || EXP2 ) FROM (" +
            "       SELECT CLAVE_ORGANO,  REGEXP_REPLACE(EXPEDIENTE_CLAVE, '[^0-9]', '') AS EXP2, COUNT(*) AS CUENTA " +
            "       FROM V3_TR_ORDINARIOJL " +
            "       GROUP BY CLAVE_ORGANO,  REGEXP_REPLACE(EXPEDIENTE_CLAVE, '[^0-9]', '') " +
            "   ) x WHERE x.CUENTA > 1" +
            ") " +
            "ORDER BY CLAVE_ORGANO, REGEXP_REPLACE(EXPEDIENTE_CLAVE, '[^0-9]', '')";

        try (
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
    // 6) Apertura < Presentación (NE) (sin filtros)
    // =========================================================
    public ArrayList<String[]> Fecha_PresentacionNE(Connection con) {
        Array = new ArrayList<>();

        sql =
            "SELECT ENTIDAD_CLAVE, CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(FECHA_APERTURA_EXPEDIENTE,'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(FECHA_PRES_DEMANDA,'dd/MM/yyyy') AS FECHA_PRES_DEMANDA " +
            "FROM V3_TR_ORDINARIOJL " +
            "WHERE FECHA_APERTURA_EXPEDIENTE < FECHA_PRES_DEMANDA " +
            "  AND FECHA_PRES_DEMANDA <> DATE '1899-09-09'";

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("ENTIDAD_CLAVE"),
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE"),
                    rs.getString("FECHA_PRES_DEMANDA")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Fecha_PresentacionNE", ex);
        }

        return Array;
    }

    // =========================================================
    // 7) Admisión < Presentación (NE) (sin filtros)
    // =========================================================
    public ArrayList<String[]> Fecha_PresentacionAdmiNE(Connection con) {
        Array = new ArrayList<>();

        sql =
            "SELECT ENTIDAD_CLAVE, CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(FECHA_ADMI_DEMANDA,'dd/MM/yyyy') AS FECHA_ADMI_DEMANDA, " +
            "       FORMATDATETIME(FECHA_PRES_DEMANDA,'dd/MM/yyyy') AS FECHA_PRES_DEMANDA " +
            "FROM V3_TR_ORDINARIOJL " +
            "WHERE FECHA_ADMI_DEMANDA < FECHA_PRES_DEMANDA " +
            "  AND FECHA_PRES_DEMANDA <> DATE '1899-09-09' " +
            "  AND FECHA_ADMI_DEMANDA <> DATE '1899-09-09'";

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("ENTIDAD_CLAVE"),
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_ADMI_DEMANDA"),
                    rs.getString("FECHA_PRES_DEMANDA")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Fecha_PresentacionAdmiNE", ex);
        }

        return Array;
    }

    // =========================================================
    // 8) Acto procesal < Apertura (NE) (sin filtros)
    // =========================================================
    public ArrayList<String[]> Fecha_Acto_ProcesalNE(Connection con) {
        Array = new ArrayList<>();

        sql =
            "SELECT ENTIDAD_CLAVE, CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(FECHA_APERTURA_EXPEDIENTE,'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(FECHA_ACTO_PROCESAL,'dd/MM/yyyy') AS FECHA_ACTO_PROCESAL " +
            "FROM V3_TR_ORDINARIOJL " +
            "WHERE FECHA_ACTO_PROCESAL <> DATE '1899-09-09' " +
            "  AND FECHA_ACTO_PROCESAL < FECHA_APERTURA_EXPEDIENTE";

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("ENTIDAD_CLAVE"),
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE"),
                    rs.getString("FECHA_ACTO_PROCESAL")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Fecha_Acto_ProcesalNE", ex);
        }

        return Array;
    }

    // =========================================================
    // 9) Admisión < Apertura (NE) (sin filtros)
    // =========================================================
    public ArrayList<String[]> Fecha_Admi_demandaNE(Connection con) {
        Array = new ArrayList<>();

        sql =
            "SELECT ENTIDAD_CLAVE, CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(FECHA_APERTURA_EXPEDIENTE,'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(FECHA_ADMI_DEMANDA,'dd/MM/yyyy') AS FECHA_ADMI_DEMANDA " +
            "FROM V3_TR_ORDINARIOJL " +
            "WHERE FECHA_ADMI_DEMANDA <> DATE '1899-09-09' " +
            "  AND FECHA_ADMI_DEMANDA < FECHA_APERTURA_EXPEDIENTE";

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("ENTIDAD_CLAVE"),
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE"),
                    rs.getString("FECHA_ADMI_DEMANDA")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Fecha_Admi_demandaNE", ex);
        }

        return Array;
    }

    // =========================================================
    // 10) Audiencia juicio <= Apertura (NE) (sin filtros)
    // =========================================================
    public ArrayList<String[]> Fecha_Audiencia_JuicioNE(Connection con) {
        Array = new ArrayList<>();

        sql =
            "SELECT ENTIDAD_CLAVE, CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(FECHA_APERTURA_EXPEDIENTE,'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(FECHA_AUDIENCIA_JUICIO,'dd/MM/yyyy') AS FECHA_AUDIENCIA_JUICIO " +
            "FROM V3_TR_ORDINARIOJL " +
            "WHERE FECHA_AUDIENCIA_JUICIO <> DATE '1899-09-09' " +
            "  AND FECHA_AUDIENCIA_JUICIO <= FECHA_APERTURA_EXPEDIENTE";

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("ENTIDAD_CLAVE"),
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE"),
                    rs.getString("FECHA_AUDIENCIA_JUICIO")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Fecha_Audiencia_JuicioNE", ex);
        }

        return Array;
    }

    // =========================================================
    // 11) Audiencia prelim <= Apertura (NE) (sin filtros)
    // =========================================================
    public ArrayList<String[]> Fecha_Audiencia_PrelimNE(Connection con) {
        Array = new ArrayList<>();

        sql =
            "SELECT ENTIDAD_CLAVE, CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(FECHA_APERTURA_EXPEDIENTE,'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(FECHA_AUDIENCIA_PRELIM,'dd/MM/yyyy') AS FECHA_AUDIENCIA_PRELIM " +
            "FROM V3_TR_ORDINARIOJL " +
            "WHERE FECHA_AUDIENCIA_PRELIM <> DATE '1899-09-09' " +
            "  AND FECHA_AUDIENCIA_PRELIM <= FECHA_APERTURA_EXPEDIENTE";

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("ENTIDAD_CLAVE"),
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE"),
                    rs.getString("FECHA_AUDIENCIA_PRELIM")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Fecha_Audiencia_PrelimNE", ex);
        }

        return Array;
    }

    // =========================================================
    // 12) Dicto Resolución FE < Apertura (NE) (sin filtros)
    // =========================================================
    public ArrayList<String[]> Fecha_Dicto_ResolucionFE(Connection con) {
        Array = new ArrayList<>();

        sql =
            "SELECT ENTIDAD_CLAVE, CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(FECHA_APERTURA_EXPEDIENTE,'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(FECHA_DICTO_RESOLUCIONFE,'dd/MM/yyyy') AS FECHA_DICTO_RESOLUCIONFE " +
            "FROM V3_TR_ORDINARIOJL " +
            "WHERE FECHA_DICTO_RESOLUCIONFE <> DATE '1899-09-09' " +
            "  AND FECHA_DICTO_RESOLUCIONFE < FECHA_APERTURA_EXPEDIENTE";

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("ENTIDAD_CLAVE"),
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE"),
                    rs.getString("FECHA_DICTO_RESOLUCIONFE")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Fecha_Dicto_ResolucionFE", ex);
        }

        return Array;
    }

    // =========================================================
    // 13) Dicto Resolución AP < Apertura (NE) (sin filtros)
    // =========================================================
    public ArrayList<String[]> Fecha_Dicto_ResolucionAP(Connection con) {
        Array = new ArrayList<>();

        sql =
            "SELECT ENTIDAD_CLAVE, CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(FECHA_APERTURA_EXPEDIENTE,'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(FECHA_DICTO_RESOLUCIONAP,'dd/MM/yyyy') AS FECHA_DICTO_RESOLUCIONAP " +
            "FROM V3_TR_ORDINARIOJL " +
            "WHERE FECHA_DICTO_RESOLUCIONAP <> DATE '1899-09-09' " +
            "  AND FECHA_DICTO_RESOLUCIONAP < FECHA_APERTURA_EXPEDIENTE";

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("ENTIDAD_CLAVE"),
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE"),
                    rs.getString("FECHA_DICTO_RESOLUCIONAP")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Fecha_Dicto_ResolucionAP", ex);
        }

        return Array;
    }

    // =========================================================
    // 14) Resolución AJ < Apertura (NE) (sin filtros)
    // =========================================================
    public ArrayList<String[]> Fecha_ResolucionAJ(Connection con) {
        Array = new ArrayList<>();

        sql =
            "SELECT ENTIDAD_CLAVE, CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(FECHA_APERTURA_EXPEDIENTE,'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(FECHA_RESOLUCIONAJ,'dd/MM/yyyy') AS FECHA_RESOLUCIONAJ " +
            "FROM V3_TR_ORDINARIOJL " +
            "WHERE FECHA_RESOLUCIONAJ <> DATE '1899-09-09' " +
            "  AND FECHA_RESOLUCIONAJ < FECHA_APERTURA_EXPEDIENTE";

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("ENTIDAD_CLAVE"),
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE"),
                    rs.getString("FECHA_RESOLUCIONAJ")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Fecha_ResolucionAJ", ex);
        }

        return Array;
    }

    // =========================================================
    // 15) Cruces con Audiencias (Ordinario tipo_proced=1)
    // =========================================================
    public ArrayList<String[]> Fecha_Aud_Presentacion(Connection con) {
        Array = new ArrayList<>();

        sql =
            "SELECT S.ENTIDAD_CLAVE, P.CLAVE_ORGANO, P.EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(P.FECHA_AUDIEN_CELEBRADA,'dd/MM/yyyy') AS FECHA_AUDIEN_CELEBRADA, " +
            "       FORMATDATETIME(S.FECHA_PRES_DEMANDA,'dd/MM/yyyy') AS FECHA_PRES_DEMANDA, " +
            "       P.ID_AUDIENCIA " +
            "FROM V3_TR_AUDIENCIASJL P " +
            "JOIN V3_TR_ORDINARIOJL S " +
            "  ON P.CLAVE_ORGANO = S.CLAVE_ORGANO " +
            " AND P.EXPEDIENTE_CLAVE = S.EXPEDIENTE_CLAVE " +
            "WHERE P.TIPO_PROCED = 1 " +
            "  AND P.FECHA_AUDIEN_CELEBRADA <> DATE '1899-09-09' " +
            "  AND S.FECHA_PRES_DEMANDA <> DATE '1899-09-09' " +
            "  AND P.FECHA_AUDIEN_CELEBRADA < S.FECHA_PRES_DEMANDA";

        try (
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

    public ArrayList<String[]> Fecha_Aud_Apertura(Connection con) {
        Array = new ArrayList<>();

        sql =
            "SELECT S.ENTIDAD_CLAVE, P.CLAVE_ORGANO, P.EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(P.FECHA_AUDIEN_CELEBRADA,'dd/MM/yyyy') AS FECHA_AUDIEN_CELEBRADA, " +
            "       FORMATDATETIME(S.FECHA_APERTURA_EXPEDIENTE,'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       P.ID_AUDIENCIA " +
            "FROM V3_TR_AUDIENCIASJL P " +
            "JOIN V3_TR_ORDINARIOJL S " +
            "  ON P.CLAVE_ORGANO = S.CLAVE_ORGANO " +
            " AND P.EXPEDIENTE_CLAVE = S.EXPEDIENTE_CLAVE " +
            "WHERE P.TIPO_PROCED = 1 " +
            "  AND P.FECHA_AUDIEN_CELEBRADA <> DATE '1899-09-09' " +
            "  AND S.FECHA_APERTURA_EXPEDIENTE <> DATE '1899-09-09' " +
            "  AND P.FECHA_AUDIEN_CELEBRADA < S.FECHA_APERTURA_EXPEDIENTE";

        try (
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

    public ArrayList<String[]> Fecha_Aud_Admision(Connection con) {
        Array = new ArrayList<>();

        sql =
            "SELECT S.ENTIDAD_CLAVE, P.CLAVE_ORGANO, P.EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(P.FECHA_AUDIEN_CELEBRADA,'dd/MM/yyyy') AS FECHA_AUDIEN_CELEBRADA, " +
            "       FORMATDATETIME(S.FECHA_ADMI_DEMANDA,'dd/MM/yyyy') AS FECHA_ADMI_DEMANDA, " +
            "       P.ID_AUDIENCIA " +
            "FROM V3_TR_AUDIENCIASJL P " +
            "JOIN V3_TR_ORDINARIOJL S " +
            "  ON P.CLAVE_ORGANO = S.CLAVE_ORGANO " +
            " AND P.EXPEDIENTE_CLAVE = S.EXPEDIENTE_CLAVE " +
            "WHERE P.TIPO_PROCED = 1 " +
            "  AND P.FECHA_AUDIEN_CELEBRADA <> DATE '1899-09-09' " +
            "  AND S.FECHA_ADMI_DEMANDA <> DATE '1899-09-09' " +
            "  AND P.FECHA_AUDIEN_CELEBRADA < S.FECHA_ADMI_DEMANDA";

        try (
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
