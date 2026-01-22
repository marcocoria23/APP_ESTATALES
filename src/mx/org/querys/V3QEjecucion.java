package mx.org.querys;

import Conexion.ConexionH2;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class V3QEjecucion {

    private Connection getCon() throws SQLException {
        return ConexionH2.getConnection();
    }

    // ----------------------------------------------------------
    // 1) Año_JudicialCampeche  (SIN filtros)
    // ----------------------------------------------------------
    public ArrayList<String[]> Año_JudicialCampeche(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, FECHA_APERTURA_EXPEDIENTES " +
            "FROM ( " +
            "  SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "         FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTES, " +
            "         SUBSTRING(FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd-MM-yyyy'), LENGTH(FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd-MM-yyyy')) - 3, 4) AS FECHA_APERTURA_EXPEDIENTE, " +
            "         (CAST(SUBSTRING(FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd-MM-yyyy'), LENGTH(FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd-MM-yyyy')) - 3, 4) AS INT) + 1) AS FECHA_APERTURA_EXPEDIENTES1, " +
            "         SUBSTRING( TRIM( " +
            "             REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(EXPEDIENTE_CLAVE,'-',''),'I',''),'J',''),'L',''),'/',''),'ll',''),'II',''),'I ','')) " +
            "         ), LENGTH(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(EXPEDIENTE_CLAVE,'-',''),'I',''),'J',''),'L',''),'/',''),'ll',''),'II',''),'I ',''))) - 3, 4) AS EXPE_ANIO " +
            "  FROM V3_TR_ejecucionjl " +
            ") X " +
            "WHERE X.FECHA_APERTURA_EXPEDIENTE <> X.EXPE_ANIO " +
            "  AND X.FECHA_APERTURA_EXPEDIENTES NOT BETWEEN " +
            "      '01/09/' || X.FECHA_APERTURA_EXPEDIENTE " +
            "      AND '01/08/' || CAST(X.FECHA_APERTURA_EXPEDIENTES1 AS VARCHAR)";

        System.out.println(sql);

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

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    // ----------------------------------------------------------
    // 2) Año_DIF_Campeche (SIN filtros)
    // ----------------------------------------------------------
    public ArrayList<String[]> Año_DIF_Campeche(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        // OJO: aquí dejé la lógica tal cual tu Oracle:
        // - calcula EXPE_AÑO con replace(...) y compara
        // - quita años "normales"
        // - y al final: EXPE_AÑO NOT IN (AñoJuridico) -> como no hay PValidacion, lo dejé sin esa parte.
        // Si quieres que conserve la lista AñoJuridico, pásamela fija o dime cómo la quieres (ej: ('2020','2021','2022'))
        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, FECHA_APERTURA_EXPEDIENTES " +
            "FROM ( " +
            "  SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "         FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTES, " +
            "         SUBSTRING(FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd-MM-yyyy'), LENGTH(FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd-MM-yyyy')) - 3, 4) AS FECHA_APERTURA_EXPEDIENTE, " +
            "         SUBSTRING( TRIM( " +
            "             REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(EXPEDIENTE_CLAVE,'-',''),'I',''),'J',''),'L',''),'/',''),'ll',''),'II',''),'I ','')) " +
            "         ), LENGTH(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(EXPEDIENTE_CLAVE,'-',''),'I',''),'J',''),'L',''),'/',''),'ll',''),'II',''),'I ',''))) - 3, 4) AS EXPE_ANIO " +
            "  FROM V3_TR_ejecucionjl " +
            ") X " +
            "WHERE X.FECHA_APERTURA_EXPEDIENTE <> X.EXPE_ANIO " +
            "  AND X.EXPE_ANIO NOT IN ('2021','2022','2023','2020','2024','2025')";

        System.out.println(sql);

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

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    // ----------------------------------------------------------
    // 3) Año_Expe_EjecucionNE (SIN filtros)
    // ----------------------------------------------------------
    public ArrayList<String[]> Año_Expe_EjecucionNE(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, FECHA_APERTURA_EXPEDIENTES " +
            "FROM ( " +
            "  SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "         FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTES, " +
            "         SUBSTRING(FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd-MM-yyyy'), LENGTH(FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd-MM-yyyy')) - 3, 4) AS FECHA_APERTURA_EXPEDIENTE, " +
            "         SUBSTRING(REGEXP_REPLACE(EXPEDIENTE_CLAVE, '[^0-9]', ''), " +
            "                   LENGTH(REGEXP_REPLACE(EXPEDIENTE_CLAVE, '[^0-9]', '')) - 3, 4) AS EXPE_ANIO " +
            "  FROM V3_TR_ejecucionjl " +
            ") X " +
            "WHERE X.FECHA_APERTURA_EXPEDIENTE <> X.EXPE_ANIO";

        System.out.println(sql);

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

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    // ----------------------------------------------------------
    // 4) FECHA_*_FUT (SIN filtros)
    // ----------------------------------------------------------
    public ArrayList<String[]> FECHA_APERTURA_EXPEDIENTE_FUT(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE,  " +
            "       FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE " +
            "FROM V3_TR_EJECUCIONJL " +
            "WHERE FECHA_APERTURA_EXPEDIENTE > CURRENT_DATE " +
            "  AND FECHA_APERTURA_EXPEDIENTE <> DATE '1899-09-09'";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    //NOTA: MODIFICAR CLASE PRINCIPAL
                    rs.getString("FECHA_APERTURA_EXPEDIENTE")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    public ArrayList<String[]> FECHA_PRESENTACION_FUT(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE,  " +
            "       FORMATDATETIME(CAST(FECHA_PRESENTACION AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_PRESENTACION " +
            "FROM V3_TR_EJECUCIONJL " +
            "WHERE FECHA_PRESENTACION > CURRENT_DATE " +
            "  AND FECHA_PRESENTACION <> DATE '1899-09-09'";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    //NOTA: MODIFICAR CLASE PRINCIPAL
                    rs.getString("FECHA_PRESENTACION")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    public ArrayList<String[]> FECHA_CONCLUSION_FUT(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE,  " +
            "       FORMATDATETIME(CAST(FECHA_CONCLUSION AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_CONCLUSION " +
            "FROM V3_TR_EJECUCIONJL " +
            "WHERE FECHA_CONCLUSION > CURRENT_DATE " +
            "  AND FECHA_CONCLUSION <> DATE '1899-09-09'";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    //NOTA: MODIFICAR CLASE PRINCIPAL
                    rs.getString("FECHA_CONCLUSION")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    // ----------------------------------------------------------
    // 5) Duplicidad_expediente (SIN filtros)
    // ----------------------------------------------------------
    public ArrayList<String[]> Duplicidad_expediente(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE " +
            "FROM V3_TR_EJECUCIONJL " +
            "WHERE CLAVE_ORGANO || CAST(REGEXP_REPLACE(EXPEDIENTE_CLAVE, '[^0-9]', '') AS BIGINT)  IN ( " +
            "  SELECT CLAVE_ORGANO || EXPEDIENTE_CLAVE2  " +
            "  FROM ( " +
            "    SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE2,  COUNT(*) AS CUENTA " +
            "    FROM ( " +
            "      SELECT CLAVE_ORGANO, " +
            "             CAST(REGEXP_REPLACE(EXPEDIENTE_CLAVE, '[^0-9]', '') AS BIGINT) AS EXPEDIENTE_CLAVE2 " +
            "      FROM V3_TR_EJECUCIONJL " +
            "    ) T " +
            "    GROUP BY CLAVE_ORGANO, EXPEDIENTE_CLAVE2 " +
            "  ) X " +
            "  WHERE CUENTA > 1 " +
            ") " +
            "ORDER BY CLAVE_ORGANO, CAST(REGEXP_REPLACE(EXPEDIENTE_CLAVE, '[^0-9]', '') AS BIGINT)";

        System.out.println(sql);

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

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    // ----------------------------------------------------------
    // 6) Fecha_PresentacionNE (tu regla: apertura < presentacion) SIN FILTROS
    // ----------------------------------------------------------
    public ArrayList<String[]> Fecha_PresentacionNE(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT ESTATUS_EXPE, CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(CAST(FECHA_PRESENTACION AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_PRESENTACION " +
            "FROM V3_TR_EJECUCIONJL " +
            "WHERE FECHA_PRESENTACION <> DATE '1899-09-09' " +
            "  AND CAST(FECHA_APERTURA_EXPEDIENTE AS DATE) < CAST(FECHA_PRESENTACION AS DATE)";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("ESTATUS_EXPE"),
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE"),
                    rs.getString("FECHA_PRESENTACION")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    // ----------------------------------------------------------
    // 7) Fecha_ConclusionNE (conclusion < apertura) SIN FILTROS
    // ----------------------------------------------------------
    public ArrayList<String[]> Fecha_ConclusionNE(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT ESTATUS_EXPE, CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(CAST(FECHA_CONCLUSION AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_CONCLUSION " +
            "FROM V3_TR_EJECUCIONJL " +
            "WHERE FECHA_CONCLUSION <> DATE '1899-09-09' " +
            "  AND CAST(FECHA_CONCLUSION AS DATE) < CAST(FECHA_APERTURA_EXPEDIENTE AS DATE)";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("ESTATUS_EXPE"),
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE"),
                    rs.getString("FECHA_CONCLUSION")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }
}
