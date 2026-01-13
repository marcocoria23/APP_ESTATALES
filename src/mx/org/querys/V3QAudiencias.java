package mx.org.querys;

import Conexion.ConexionH2;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class V3QAudiencias {

    private Connection getCon() throws SQLException {
        return ConexionH2.getConnection();
    }

    private String tipoProcedTexto() {
        return "CASE TIPO_PROCED " +
               " WHEN '1' THEN 'Ordinario' " +
               " WHEN '2' THEN 'Especial Individual' " +
               " WHEN '3' THEN 'Especial Colectivo' " +
               " WHEN '4' THEN 'Huelga' " +
               " WHEN '5' THEN 'Colectivo Ecónomica' " +
               " WHEN '9' THEN 'No identificado' " +
               " ELSE TIPO_PROCED END";
    }

    // ----------------------------------------------------------
    // FECHA_AUDIEN_CELEBRADA_FUT (sin filtros)
    // ----------------------------------------------------------
    public ArrayList<String[]> FECHA_AUDIEN_CELEBRADA_FUT() {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, PERIODO, " +
            "       FORMATDATETIME(CAST(FECHA_AUDIEN_CELEBRADA AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_AUDIEN_CELEBRADA " +
            "FROM V3_TR_AUDIENCIASJL " +
            "WHERE FECHA_AUDIEN_CELEBRADA > CURRENT_DATE " +
            "  AND FECHA_AUDIEN_CELEBRADA <> DATE '1899-09-09'";

        System.out.println(sql);

        try (Connection con = getCon();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("PERIODO"),
                    rs.getString("FECHA_AUDIEN_CELEBRADA")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    // ----------------------------------------------------------
    // FORMATO_INICIO (sin filtros)
    // ----------------------------------------------------------
    public ArrayList<String[]> FORMATO_INICIO() {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, ID_AUDIENCIA, " + tipoProcedTexto() + " AS TIPO_PROCED, " +
            "       INICIO " +
            "FROM ( " +
            "   SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, ID_AUDIENCIA, TIPO_PROCED, INICIO, PERIODO, " +
            "          CASE " +
            "            WHEN LENGTH(REGEXP_REPLACE(INICIO, '[^0-9]', '')) = 4 THEN SUBSTRING(REGEXP_REPLACE(INICIO, '[^0-9]', ''), 1, 2) " +
            "            WHEN LENGTH(REGEXP_REPLACE(INICIO, '[^0-9]', '')) = 3 THEN SUBSTRING(REGEXP_REPLACE(INICIO, '[^0-9]', ''), 1, 1) " +
            "          END AS CAM_INICIO " +
            "   FROM V3_TR_AUDIENCIASJL " +
            ") X " +
            "WHERE (INICIO NOT LIKE '%:%' OR LENGTH(TRIM(INICIO)) < 5 OR CAM_INICIO IN ('1','01','2','02','3','03','4','04','5','05','6','06'))";

        System.out.println(sql);

        try (Connection con = getCon();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("ID_AUDIENCIA"),
                    rs.getString("TIPO_PROCED"),
                    rs.getString("INICIO")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    // ----------------------------------------------------------
    // FORMATO_CONCLU (sin filtros)
    // ----------------------------------------------------------
    public ArrayList<String[]> FORMATO_CONCLU() {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, ID_AUDIENCIA, " + tipoProcedTexto() + " AS TIPO_PROCED, " +
            "       CONCLU " +
            "FROM ( " +
            "   SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, ID_AUDIENCIA, TIPO_PROCED, CONCLU, PERIODO, " +
            "          CASE " +
            "            WHEN LENGTH(REGEXP_REPLACE(CONCLU, '[^0-9]', '')) = 4 THEN SUBSTRING(REGEXP_REPLACE(CONCLU, '[^0-9]', ''), 1, 2) " +
            "            WHEN LENGTH(REGEXP_REPLACE(CONCLU, '[^0-9]', '')) = 3 THEN SUBSTRING(REGEXP_REPLACE(CONCLU, '[^0-9]', ''), 1, 1) " +
            "          END AS CAM_CONCLU " +
            "   FROM V3_TR_AUDIENCIASJL " +
            ") X " +
            "WHERE (CONCLU NOT LIKE '%:%' OR LENGTH(TRIM(CONCLU)) < 5 OR CAM_CONCLU IN ('1','01','2','02','3','03','4','04','5','05','6','06'))";

        System.out.println(sql);

        try (Connection con = getCon();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("ID_AUDIENCIA"),
                    rs.getString("TIPO_PROCED"),
                    rs.getString("CONCLU")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    // ----------------------------------------------------------
    // SEGUNDOS_INICIO (sin filtros)
    // ----------------------------------------------------------
    public ArrayList<String[]> SEGUNDOS_INICIO() {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, ID_AUDIENCIA, " + tipoProcedTexto() + " AS TIPO_PROCED, INICIO " +
            "FROM V3_TR_AUDIENCIASJL " +
            "WHERE (SUBSTRING(REGEXP_REPLACE(INICIO, '[^0-9]', ''), LENGTH(REGEXP_REPLACE(INICIO, '[^0-9]', '')) - 1, 2) > '59') " +
            "  AND INICIO NOT IN ('99:99')";

        System.out.println(sql);

        try (Connection con = getCon();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("ID_AUDIENCIA"),
                    rs.getString("TIPO_PROCED"),
                    rs.getString("INICIO")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    // ----------------------------------------------------------
    // SEGUNDOS_CONCLU (sin filtros)
    // ----------------------------------------------------------
    public ArrayList<String[]> SEGUNDOS_CONCLU() {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, ID_AUDIENCIA, " + tipoProcedTexto() + " AS TIPO_PROCED, CONCLU " +
            "FROM V3_TR_AUDIENCIASJL " +
            "WHERE (SUBSTRING(REGEXP_REPLACE(CONCLU, '[^0-9]', ''), LENGTH(REGEXP_REPLACE(CONCLU, '[^0-9]', '')) - 1, 2) > '59') " +
            "  AND CONCLU NOT IN ('99:99')";

        System.out.println(sql);

        try (Connection con = getCon();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("ID_AUDIENCIA"),
                    rs.getString("TIPO_PROCED"),
                    rs.getString("CONCLU")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    // ----------------------------------------------------------
    // CONCLU_MENOR (sin filtros)
    // ----------------------------------------------------------
    public ArrayList<String[]> CONCLU_MENOR() {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, ID_AUDIENCIA, " + tipoProcedTexto() + " AS TIPO_PROCED, INICIO, CONCLU " +
            "FROM ( " +
            "  SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, ID_AUDIENCIA, TIPO_PROCED, PERIODO, " +
            "         CASE WHEN LENGTH(INICIO) < 5 THEN CONCAT('0', INICIO) ELSE INICIO END AS INICIO, " +
            "         CASE WHEN LENGTH(CONCLU) < 5 THEN CONCAT('0', CONCLU) ELSE CONCLU END AS CONCLU " +
            "  FROM V3_TR_AUDIENCIASJL " +
            "  WHERE INICIO LIKE '%:%' AND CONCLU LIKE '%:%' " +
            "    AND INICIO NOT IN ('99:99') AND CONCLU NOT IN ('99:99') " +
            ") X " +
            "WHERE CAST(PARSEDATETIME(X.CONCLU, 'HH:mm') AS TIME) < CAST(PARSEDATETIME(X.INICIO, 'HH:mm') AS TIME)";

        System.out.println(sql);

        try (Connection con = getCon();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("ID_AUDIENCIA"),
                    rs.getString("TIPO_PROCED"),
                    rs.getString("INICIO"),
                    rs.getString("CONCLU")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }
}
