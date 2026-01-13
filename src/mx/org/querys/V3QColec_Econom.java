package mx.org.querys;

import Conexion.ConexionH2;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class V3QColec_Econom {

    private Connection getCon() throws SQLException {
        return ConexionH2.getConnection();
    }

    // ----------------------------------------------------------------
    // Año_JudicialCampeche (H2 + sin filtros)
    // ----------------------------------------------------------------
    public ArrayList<String[]> Año_JudicialCampeche() {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, FECHA_APERTURA_EXPEDIENTES " +
            "FROM ( " +
            "  SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "         FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTES, " +
            "         RIGHT(FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd-MM-yyyy'), 4) AS FECHA_APERTURA_ANIO, " +
            "         (CAST(RIGHT(FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd-MM-yyyy'), 4) AS INT) + 1) AS FECHA_APERTURA_ANIO_MAS1, " +
            "         RIGHT(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(EXPEDIENTE_CLAVE, '-', ''), 'I', ''), 'J', ''), 'L', ''), '/', ''), 'll', ''), 'II', ''), 'I ', '')), 4) AS EXPE_ANIO " +
            "  FROM V3_TR_COLECT_ECONOMJL " +
            ") X " +
            "WHERE X.FECHA_APERTURA_ANIO <> X.EXPE_ANIO " +
            "  AND CAST(PARSEDATETIME('01/09/' || X.FECHA_APERTURA_ANIO, 'dd/MM/yyyy') AS DATE) <= CAST(PARSEDATETIME(X.FECHA_APERTURA_EXPEDIENTES, 'dd/MM/yyyy') AS DATE) " +
            "  AND CAST(PARSEDATETIME(X.FECHA_APERTURA_EXPEDIENTES, 'dd/MM/yyyy') AS DATE) <  CAST(PARSEDATETIME('01/08/' || X.FECHA_APERTURA_ANIO_MAS1, 'dd/MM/yyyy') AS DATE)";

        System.out.println(sql);

        try (Connection con = getCon();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTES")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    // ----------------------------------------------------------------
    // Año_DIF_Campeche (H2 + sin filtros)
    // OJO: aquí antes usabas PValidacion.AñoJuridico (lista). Sin eso,
    // dejo un ejemplo fijo (ajústalo si quieres).
    // ----------------------------------------------------------------
    public ArrayList<String[]> Año_DIF_Campeche() {
        ArrayList<String[]> Array = new ArrayList<>();

        // Ajusta la lista según lo que usabas en PValidacion.AñoJuridico
        String listaAnios = "('2020','2021','2022')";

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, FECHA_APERTURA_EXPEDIENTES " +
            "FROM ( " +
            "  SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "         FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTES, " +
            "         RIGHT(FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd-MM-yyyy'), 4) AS FECHA_APERTURA_ANIO, " +
            "         RIGHT(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(EXPEDIENTE_CLAVE, '-', ''), 'I', ''), 'J', ''), 'L', ''), '/', ''), 'll', ''), 'II', ''), 'I ', '')), 4) AS EXPE_ANIO " +
            "  FROM V3_TR_COLECT_ECONOMJL " +
            ") X " +
            "WHERE X.FECHA_APERTURA_ANIO <> X.EXPE_ANIO " +
            "  AND X.EXPE_ANIO NOT IN " + listaAnios;

        System.out.println(sql);

        try (Connection con = getCon();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTES")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    // ----------------------------------------------------------------
    // Año_Expe_ColecEconomNE (H2 + sin filtros)
    // ----------------------------------------------------------------
    public ArrayList<String[]> Año_Expe_ColecEconomNE() {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, FECHA_APERTURA_EXPEDIENTES " +
            "FROM ( " +
            "  SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "         FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTES, " +
            "         RIGHT(FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd-MM-yyyy'), 4) AS FECHA_APERTURA_ANIO, " +
            "         RIGHT(REGEXP_REPLACE(EXPEDIENTE_CLAVE, '[^0-9]', ''), 4) AS EXPE_ANIO " +
            "  FROM V3_TR_COLECT_ECONOMJL " +
            ") X " +
            "WHERE X.FECHA_APERTURA_ANIO <> X.EXPE_ANIO " +
            "  AND X.EXPE_ANIO NOT IN ('2020','2021','2022','2023','2024','2025')";

        System.out.println(sql);

        try (Connection con = getCon();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTES")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    // ----------------------------------------------------------------
    // FECHAS FUTURAS (H2 + sin filtros)
    // ----------------------------------------------------------------
    public ArrayList<String[]> FECHA_APERTURA_EXPEDIENTE_FUT() {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, PERIODO, " +
            "       FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE " +
            "FROM V3_TR_COLECT_ECONOMJL " +
            "WHERE FECHA_APERTURA_EXPEDIENTE > CURRENT_DATE " +
            "  AND FECHA_APERTURA_EXPEDIENTE <> DATE '1899-09-09'";

        System.out.println(sql);

        try (Connection con = getCon();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("PERIODO"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    public ArrayList<String[]> FECHA_PRES_DEMANDA_FUT() {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, PERIODO, " +
            "       FORMATDATETIME(CAST(FECHA_PRES_DEMANDA AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_PRES_DEMANDA " +
            "FROM V3_TR_COLECT_ECONOMJL " +
            "WHERE FECHA_PRES_DEMANDA > CURRENT_DATE " +
            "  AND FECHA_PRES_DEMANDA <> DATE '1899-09-09'";

        System.out.println(sql);

        try (Connection con = getCon();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("PERIODO"),
                    rs.getString("FECHA_PRES_DEMANDA")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    public ArrayList<String[]> FECHA_ADMISION_DEMANDA_FUT() {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, PERIODO, " +
            "       FORMATDATETIME(CAST(FECHA_ADMISION_DEMANDA AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_ADMISION_DEMANDA " +
            "FROM V3_TR_COLECT_ECONOMJL " +
            "WHERE FECHA_ADMISION_DEMANDA > CURRENT_DATE " +
            "  AND FECHA_ADMISION_DEMANDA <> DATE '1899-09-09'";

        System.out.println(sql);

        try (Connection con = getCon();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("PERIODO"),
                    rs.getString("FECHA_ADMISION_DEMANDA")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    public ArrayList<String[]> FECHA_AUDIENCIA_ECONOM_FUT() {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, PERIODO, " +
            "       FORMATDATETIME(CAST(FECHA_AUDIENCIA_ECONOM AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_AUDIENCIA_ECONOM " +
            "FROM V3_TR_COLECT_ECONOMJL " +
            "WHERE FECHA_AUDIENCIA_ECONOM > CURRENT_DATE " +
            "  AND FECHA_AUDIENCIA_ECONOM <> DATE '1899-09-09'";

        System.out.println(sql);

        try (Connection con = getCon();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("PERIODO"),
                    rs.getString("FECHA_AUDIENCIA_ECONOM")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    public ArrayList<String[]> FECHA_ACTO_PROCESAL_FUT() {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, PERIODO, " +
            "       FORMATDATETIME(CAST(FECHA_ACTO_PROCESAL AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_ACTO_PROCESAL " +
            "FROM V3_TR_COLECT_ECONOMJL " +
            "WHERE FECHA_ACTO_PROCESAL > CURRENT_DATE " +
            "  AND FECHA_ACTO_PROCESAL <> DATE '1899-09-09'";

        System.out.println(sql);

        try (Connection con = getCon();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("PERIODO"),
                    rs.getString("FECHA_ACTO_PROCESAL")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    public ArrayList<String[]> FECHA_RESOLUCION_FUT() {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, PERIODO, " +
            "       FORMATDATETIME(CAST(FECHA_RESOLUCION AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_RESOLUCION " +
            "FROM V3_TR_COLECT_ECONOMJL " +
            "WHERE FECHA_RESOLUCION > CURRENT_DATE " +
            "  AND FECHA_RESOLUCION <> DATE '1899-09-09'";

        System.out.println(sql);

        try (Connection con = getCon();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("PERIODO"),
                    rs.getString("FECHA_RESOLUCION")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    // ----------------------------------------------------------------
    // Duplicidad_expediente (H2 + sin filtros)
    // ----------------------------------------------------------------
    public ArrayList<String[]> Duplicidad_expediente() {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE " +
            "FROM V3_TR_COLECT_ECONOMJL T " +
            "WHERE (CLAVE_ORGANO || REGEXP_REPLACE(EXPEDIENTE_CLAVE, '[^0-9]', '') || PERIODO) IN ( " +
            "   SELECT (CLAVE_ORGANO || EXPEDIENTE_NUM || PERIODO) FROM ( " +
            "      SELECT CLAVE_ORGANO, PERIODO, REGEXP_REPLACE(EXPEDIENTE_CLAVE, '[^0-9]', '') AS EXPEDIENTE_NUM, COUNT(*) AS C " +
            "      FROM V3_TR_COLECT_ECONOMJL " +
            "      GROUP BY CLAVE_ORGANO, PERIODO, REGEXP_REPLACE(EXPEDIENTE_CLAVE, '[^0-9]', '') " +
            "   ) X WHERE X.C > 1 " +
            ") " +
            "ORDER BY CLAVE_ORGANO, REGEXP_REPLACE(EXPEDIENTE_CLAVE, '[^0-9]', '')";

        System.out.println(sql);

        try (Connection con = getCon();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    // ----------------------------------------------------------------
    // Fecha_PresentacionNE (H2 + sin filtros)
    // Apertura < Presentación (y presentación != 1899-09-09)
    // ----------------------------------------------------------------
    public ArrayList<String[]> Fecha_PresentacionNE() {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT ENTIDAD_CLAVE, CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(CAST(FECHA_PRES_DEMANDA AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_PRES_DEMANDA " +
            "FROM V3_TR_COLECT_ECONOMJL " +
            "WHERE FECHA_PRES_DEMANDA <> DATE '1899-09-09' " +
            "  AND CAST(FECHA_APERTURA_EXPEDIENTE AS DATE) < CAST(FECHA_PRES_DEMANDA AS DATE)";

        System.out.println(sql);

        try (Connection con = getCon();
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

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    // ----------------------------------------------------------------
    // Fecha_PresentacionAdmiNE (H2 + sin filtros)
    // Admisión < Presentación
    // ----------------------------------------------------------------
    public ArrayList<String[]> Fecha_PresentacionAdmiNE() {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT ENTIDAD_CLAVE, CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(CAST(FECHA_ADMISION_DEMANDA AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_ADMISION_DEMANDA, " +
            "       FORMATDATETIME(CAST(FECHA_PRES_DEMANDA AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_PRES_DEMANDA " +
            "FROM V3_TR_COLECT_ECONOMJL " +
            "WHERE FECHA_PRES_DEMANDA <> DATE '1899-09-09' " +
            "  AND FECHA_ADMISION_DEMANDA <> DATE '1899-09-09' " +
            "  AND CAST(FECHA_ADMISION_DEMANDA AS DATE) < CAST(FECHA_PRES_DEMANDA AS DATE)";

        System.out.println(sql);

        try (Connection con = getCon();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("ENTIDAD_CLAVE"),
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_ADMISION_DEMANDA"),
                    rs.getString("FECHA_PRES_DEMANDA")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    // ----------------------------------------------------------------
    // Fecha_Admision_DemandaNE (H2 + sin filtros)
    // Admisión < Apertura
    // ----------------------------------------------------------------
    public ArrayList<String[]> Fecha_Admision_DemandaNE() {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT ENTIDAD_CLAVE, CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(CAST(FECHA_ADMISION_DEMANDA AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_ADMISION_DEMANDA " +
            "FROM V3_TR_COLECT_ECONOMJL " +
            "WHERE FECHA_ADMISION_DEMANDA <> DATE '1899-09-09' " +
            "  AND CAST(FECHA_ADMISION_DEMANDA AS DATE) < CAST(FECHA_APERTURA_EXPEDIENTE AS DATE)";

        System.out.println(sql);

        try (Connection con = getCon();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("ENTIDAD_CLAVE"),
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE"),
                    rs.getString("FECHA_ADMISION_DEMANDA")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    // ----------------------------------------------------------------
    // Fecha_audiencia_EconomNE (H2 + sin filtros)
    // Audiencia <= Apertura
    // ----------------------------------------------------------------
    public ArrayList<String[]> Fecha_audiencia_EconomNE() {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT ENTIDAD_CLAVE, CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(CAST(FECHA_AUDIENCIA_ECONOM AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_AUDIENCIA_ECONOM " +
            "FROM V3_TR_COLECT_ECONOMJL " +
            "WHERE FECHA_AUDIENCIA_ECONOM <> DATE '1899-09-09' " +
            "  AND CAST(FECHA_AUDIENCIA_ECONOM AS DATE) <= CAST(FECHA_APERTURA_EXPEDIENTE AS DATE)";

        System.out.println(sql);

        try (Connection con = getCon();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("ENTIDAD_CLAVE"),
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE"),
                    rs.getString("FECHA_AUDIENCIA_ECONOM")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    // ----------------------------------------------------------------
    // Fecha_acto_procesalNE (H2 + sin filtros)
    // Acto procesal < Apertura
    // ----------------------------------------------------------------
    public ArrayList<String[]> Fecha_acto_procesalNE() {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT ENTIDAD_CLAVE, CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(CAST(FECHA_ACTO_PROCESAL AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_ACTO_PROCESAL " +
            "FROM V3_TR_COLECT_ECONOMJL " +
            "WHERE FECHA_ACTO_PROCESAL <> DATE '1899-09-09' " +
            "  AND CAST(FECHA_ACTO_PROCESAL AS DATE) < CAST(FECHA_APERTURA_EXPEDIENTE AS DATE)";

        System.out.println(sql);

        try (Connection con = getCon();
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

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    // ----------------------------------------------------------------
    // Fecha_resolucionNE (H2 + sin filtros)
    // Resolución < Apertura
    // ----------------------------------------------------------------
    public ArrayList<String[]> Fecha_resolucionNE() {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT ENTIDAD_CLAVE, CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(CAST(FECHA_RESOLUCION AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_RESOLUCION " +
            "FROM V3_TR_COLECT_ECONOMJL " +
            "WHERE FECHA_RESOLUCION <> DATE '1899-09-09' " +
            "  AND CAST(FECHA_RESOLUCION AS DATE) < CAST(FECHA_APERTURA_EXPEDIENTE AS DATE)";

        System.out.println(sql);

        try (Connection con = getCon();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("ENTIDAD_CLAVE"),
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE"),
                    rs.getString("FECHA_RESOLUCION")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    // ----------------------------------------------------------------
    // Fecha_Aud_Presentacion (JOIN Audiencias vs Colectiva) (H2 + sin filtros)
    // FECHA_AUDIEN_CELEBRADA < FECHA_PRES_DEMANDA
    // ----------------------------------------------------------------
    public ArrayList<String[]> Fecha_Aud_Presentacion() {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT S.ENTIDAD_CLAVE, P.CLAVE_ORGANO, P.EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(CAST(P.FECHA_AUDIEN_CELEBRADA AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_AUDIEN_CELEBRADA, " +
            "       FORMATDATETIME(CAST(S.FECHA_PRES_DEMANDA AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_PRES_DEMANDA, " +
            "       P.ID_AUDIENCIA " +
            "FROM V3_TR_AUDIENCIASJL P " +
            "JOIN V3_TR_COLECT_ECONOMJL S " +
            "  ON P.CLAVE_ORGANO = S.CLAVE_ORGANO " +
            " AND P.EXPEDIENTE_CLAVE = S.EXPEDIENTE_CLAVE " +
            " AND P.PERIODO = S.PERIODO " +
            "WHERE P.TIPO_PROCED = '5' " +
            "  AND P.FECHA_AUDIEN_CELEBRADA <> DATE '1899-09-09' " +
            "  AND S.FECHA_PRES_DEMANDA <> DATE '1899-09-09' " +
            "  AND CAST(P.FECHA_AUDIEN_CELEBRADA AS DATE) < CAST(S.FECHA_PRES_DEMANDA AS DATE)";

        System.out.println(sql);

        try (Connection con = getCon();
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

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    // ----------------------------------------------------------------
    // Fecha_Aud_Apertura (H2 + sin filtros)
    // FECHA_AUDIEN_CELEBRADA < FECHA_APERTURA_EXPEDIENTE
    // ----------------------------------------------------------------
    public ArrayList<String[]> Fecha_Aud_Apertura() {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT S.ENTIDAD_CLAVE, P.CLAVE_ORGANO, P.EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(CAST(P.FECHA_AUDIEN_CELEBRADA AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_AUDIEN_CELEBRADA, " +
            "       FORMATDATETIME(CAST(S.FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       P.ID_AUDIENCIA " +
            "FROM V3_TR_AUDIENCIASJL P " +
            "JOIN V3_TR_COLECT_ECONOMJL S " +
            "  ON P.CLAVE_ORGANO = S.CLAVE_ORGANO " +
            " AND P.EXPEDIENTE_CLAVE = S.EXPEDIENTE_CLAVE " +
            " AND P.PERIODO = S.PERIODO " +
            "WHERE P.TIPO_PROCED = '5' " +
            "  AND P.FECHA_AUDIEN_CELEBRADA <> DATE '1899-09-09' " +
            "  AND S.FECHA_APERTURA_EXPEDIENTE <> DATE '1899-09-09' " +
            "  AND CAST(P.FECHA_AUDIEN_CELEBRADA AS DATE) < CAST(S.FECHA_APERTURA_EXPEDIENTE AS DATE)";

        System.out.println(sql);

        try (Connection con = getCon();
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

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    // ----------------------------------------------------------------
    // Fecha_Aud_Admision (H2 + sin filtros)
    // FECHA_AUDIEN_CELEBRADA < FECHA_ADMISION_DEMANDA
    // ----------------------------------------------------------------
    public ArrayList<String[]> Fecha_Aud_Admision() {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT S.ENTIDAD_CLAVE, P.CLAVE_ORGANO, P.EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(CAST(P.FECHA_AUDIEN_CELEBRADA AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_AUDIEN_CELEBRADA, " +
            "       FORMATDATETIME(CAST(S.FECHA_ADMISION_DEMANDA AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_ADMISION_DEMANDA, " +
            "       P.ID_AUDIENCIA " +
            "FROM V3_TR_AUDIENCIASJL P " +
            "JOIN V3_TR_COLECT_ECONOMJL S " +
            "  ON P.CLAVE_ORGANO = S.CLAVE_ORGANO " +
            " AND P.EXPEDIENTE_CLAVE = S.EXPEDIENTE_CLAVE " +
            " AND P.PERIODO = S.PERIODO " +
            "WHERE P.TIPO_PROCED = '5' " +
            "  AND P.FECHA_AUDIEN_CELEBRADA <> DATE '1899-09-09' " +
            "  AND S.FECHA_ADMISION_DEMANDA <> DATE '1899-09-09' " +
            "  AND CAST(P.FECHA_AUDIEN_CELEBRADA AS DATE) < CAST(S.FECHA_ADMISION_DEMANDA AS DATE)";

        System.out.println(sql);

        try (Connection con = getCon();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("ENTIDAD_CLAVE"),
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_AUDIEN_CELEBRADA"),
                    rs.getString("FECHA_ADMISION_DEMANDA"),
                    rs.getString("ID_AUDIENCIA")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    // ----------------------------------------------------------------
    // SinMotivo_Conflicto (H2 + sin filtros)
    // ----------------------------------------------------------------
    public ArrayList<String[]> SinMotivo_Conflicto() {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE " +
            "FROM V3_TR_COLECT_ECONOMJL " +
            "WHERE MODIF_CONDICIONES = 2 " +
            "  AND NUEVAS_CONDICIONES = 2 " +
            "  AND SUSPENSION_TEMPORAL = 2 " +
            "  AND TERMINACION_COLECTIVA = 2 " +
            "  AND OTRO_MOTIVO_ECONOM = 2";

        System.out.println(sql);

        try (Connection con = getCon();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }
}
