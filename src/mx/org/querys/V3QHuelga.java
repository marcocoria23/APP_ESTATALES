package mx.org.querys;

import Conexion.ConexionH2;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class V3QHuelga {

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
            "         SUBSTRING(FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd-MM-yyyy'), " +
            "                   LENGTH(FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd-MM-yyyy')) - 3, 4) AS FECHA_APERTURA_EXPEDIENTE, " +
            "         (CAST(SUBSTRING(FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd-MM-yyyy'), " +
            "                        LENGTH(FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd-MM-yyyy')) - 3, 4) AS INT) + 1) AS FECHA_APERTURA_EXPEDIENTES1, " +
            "         SUBSTRING(TRIM( " +
            "             REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(EXPEDIENTE_CLAVE,'-',''),'I',''),'J',''),'L',''),'/',''),'ll',''),'II',''),'I ','')) " +
            "         ), LENGTH(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(EXPEDIENTE_CLAVE,'-',''),'I',''),'J',''),'L',''),'/',''),'ll',''),'II',''),'I ',''))) - 3, 4) AS EXPE_ANIO " +
            "  FROM V3_TR_huelgajl " +
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
    //    Nota: quitamos PValidacion.AñoJuridico
    // ----------------------------------------------------------
    public ArrayList<String[]> Año_DIF_Campeche(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, FECHA_APERTURA_EXPEDIENTES " +
            "FROM ( " +
            "  SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "         FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTES, " +
            "         SUBSTRING(FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd-MM-yyyy'), " +
            "                   LENGTH(FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd-MM-yyyy')) - 3, 4) AS FECHA_APERTURA_EXPEDIENTE, " +
            "         SUBSTRING(TRIM( " +
            "             REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(EXPEDIENTE_CLAVE,'-',''),'I',''),'J',''),'L',''),'/',''),'ll',''),'II',''),'I ','')) " +
            "         ), LENGTH(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(EXPEDIENTE_CLAVE,'-',''),'I',''),'J',''),'L',''),'/',''),'ll',''),'II',''),'I ',''))) - 3, 4) AS EXPE_ANIO " +
            "  FROM V3_TR_huelgajl " +
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
    // 3) Año_Expe_HuelgaNE (SIN filtros)
    // ----------------------------------------------------------
    public ArrayList<String[]> Año_Expe_HuelgaNE(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, FECHA_APERTURA_EXPEDIENTES " +
            "FROM ( " +
            "  SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "         FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTES, " +
            "         SUBSTRING(FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd-MM-yyyy'), " +
            "                   LENGTH(FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd-MM-yyyy')) - 3, 4) AS FECHA_APERTURA_EXPEDIENTE, " +
            "         SUBSTRING(REGEXP_REPLACE(EXPEDIENTE_CLAVE, '[^0-9]', ''), " +
            "                   LENGTH(REGEXP_REPLACE(EXPEDIENTE_CLAVE, '[^0-9]', '')) - 3, 4) AS EXPE_ANIO " +
            "  FROM V3_TR_huelgajl " +
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
    // 4) FECHAS FUTURAS (SIN filtros)
    // ----------------------------------------------------------
    public ArrayList<String[]> FECHA_APERTURA_EXPEDIENTE_FUT(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE,  " +
            "       FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'DD/MM/YYYY') AS FECHA_APERTURA_EXPEDIENTE " +
            "FROM V3_TR_HUELGAJL " +
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
                    //NOTA:MODIFICAR CLASE PRINCIPAL
                    rs.getString("FECHA_APERTURA_EXPEDIENTE")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    public ArrayList<String[]> FECHA_PRESENTA_PETIC_FUT(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE,  " +
            "       FORMATDATETIME(CAST(FECHA_PRESENTA_PETIC AS TIMESTAMP), 'DD/MM/YYYY') AS FECHA_PRESENTA_PETIC " +
            "FROM V3_TR_HUELGAJL " +
            "WHERE FECHA_PRESENTA_PETIC > CURRENT_DATE " +
            "  AND FECHA_PRESENTA_PETIC <> DATE '1899-09-09'";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    //NOTA:MODIFICAR CLASE PRINCIPAL
                    rs.getString("FECHA_PRESENTA_PETIC")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    public ArrayList<String[]> FECHA_EMPLAZAMIENTO_FUT(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE,  " +
            "       FORMATDATETIME(CAST(FECHA_EMPLAZAMIENTO AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_EMPLAZAMIENTO " +
            "FROM V3_TR_HUELGAJL " +
            "WHERE FECHA_EMPLAZAMIENTO > CURRENT_DATE " +
            "  AND FECHA_EMPLAZAMIENTO <> DATE '1899-09-09'";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    //NOTA:MODIFICAR CLASE PRINCIPAL
                    rs.getString("FECHA_EMPLAZAMIENTO")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    public ArrayList<String[]> FECHA_AUDIENCIA_FUT(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE,  " +
            "       FORMATDATETIME(CAST(FECHA_AUDIENCIA AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_AUDIENCIA " +
            "FROM V3_TR_HUELGAJL " +
            "WHERE FECHA_AUDIENCIA > CURRENT_DATE " +
            "  AND FECHA_AUDIENCIA <> DATE '1899-09-09'";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    //NOTA:MODIFICAR CLASE PRINCIPAL
                    rs.getString("FECHA_AUDIENCIA")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    public ArrayList<String[]> FECHA_ACTO_PROCESAL_FUT(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE,  " +
            "       FORMATDATETIME(CAST(FECHA_ACTO_PROCESAL AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_ACTO_PROCESAL " +
            "FROM V3_TR_HUELGAJL " +
            "WHERE FECHA_ACTO_PROCESAL > CURRENT_DATE " +
            "  AND FECHA_ACTO_PROCESAL <> DATE '1899-09-09'";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    //NOTA:MODIFICAR CLASE PRINCIPAL
                    rs.getString("FECHA_ACTO_PROCESAL")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    public ArrayList<String[]> FECHA_RESOLU_EMPLAZ_FUT(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE,  " +
            "       FORMATDATETIME(CAST(FECHA_RESOLU_EMPLAZ AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_RESOLU_EMPLAZ " +
            "FROM V3_TR_HUELGAJL " +
            "WHERE FECHA_RESOLU_EMPLAZ > CURRENT_DATE " +
            "  AND FECHA_RESOLU_EMPLAZ <> DATE '1899-09-09'";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    //NOTA:MODIFICAR CLASE PRINCIPAL
                    rs.getString("FECHA_RESOLU_EMPLAZ")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    public ArrayList<String[]> FECHA_RESOLU_HUELGA_FUT(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE,  " +
            "       FORMATDATETIME(CAST(FECHA_RESOLU_HUELGA AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_RESOLU_HUELGA " +
            "FROM V3_TR_HUELGAJL " +
            "WHERE FECHA_RESOLU_HUELGA > CURRENT_DATE " +
            "  AND FECHA_RESOLU_HUELGA <> DATE '1899-09-09'";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    //NOTA:MODIFICAR CLASE PRINCIPAL
                    rs.getString("FECHA_RESOLU_HUELGA")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    public ArrayList<String[]> FECHA_ESTALLAM_HUELGA_FUT(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE,  " +
            "       FORMATDATETIME(CAST(FECHA_ESTALLAM_HUELGA AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_ESTALLAM_HUELGA " +
            "FROM V3_TR_HUELGAJL " +
            "WHERE FECHA_ESTALLAM_HUELGA > CURRENT_DATE " +
            "  AND FECHA_ESTALLAM_HUELGA <> DATE '1899-09-09'";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    //NOTA:MODIFICAR CLASE PRINCIPAL
                    rs.getString("FECHA_ESTALLAM_HUELGA")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    public ArrayList<String[]> FECHA_LEVANT_HUELGA_FUT(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE,  " +
            "       FORMATDATETIME(CAST(FECHA_LEVANT_HUELGA AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_LEVANT_HUELGA " +
            "FROM V3_TR_HUELGAJL " +
            "WHERE FECHA_LEVANT_HUELGA > CURRENT_DATE " +
            "  AND FECHA_LEVANT_HUELGA <> DATE '1899-09-09'";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    //NOTA:MODIFICAR CLASE PRINCIPAL
                    rs.getString("FECHA_LEVANT_HUELGA")
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
            "FROM V3_TR_HUELGAJL " +
            "WHERE CLAVE_ORGANO || CAST(REGEXP_REPLACE(EXPEDIENTE_CLAVE, '[^0-9]', '') AS BIGINT)  IN ( " +
            "  SELECT CLAVE_ORGANO || EXPEDIENTE_CLAVE2  " +
            "  FROM ( " +
            "    SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE2,  COUNT(*) AS CUENTA " +
            "    FROM ( " +
            "      SELECT CLAVE_ORGANO, " +
            "             CAST(REGEXP_REPLACE(EXPEDIENTE_CLAVE, '[^0-9]', '') AS BIGINT) AS EXPEDIENTE_CLAVE2 " +
            "      FROM V3_TR_HUELGAJL " +
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
    // 6) Fecha_PresentacionNE (apertura < presenta_petic) SIN filtros
    // ----------------------------------------------------------
    public ArrayList<String[]> Fecha_PresentacionNE(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT ENTIDAD_CLAVE, CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'DD/MM/YYYY') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(CAST(FECHA_PRESENTA_PETIC AS TIMESTAMP), 'DD/MM/YYYY') AS FECHA_PRESENTA_PETIC " +
            "FROM V3_TR_HUELGAJL " +
            "WHERE FECHA_PRESENTA_PETIC <> DATE '1899-09-09' " +
            "  AND CAST(FECHA_APERTURA_EXPEDIENTE AS DATE) < CAST(FECHA_PRESENTA_PETIC AS DATE)";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("ENTIDAD_CLAVE"),
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE"),
                    rs.getString("FECHA_PRESENTA_PETIC")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    // ----------------------------------------------------------
    // 7) Validaciones de fechas (SIN filtros)
    // ----------------------------------------------------------
    public ArrayList<String[]> Fecha_EmplazamientoNE(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT ENTIDAD_CLAVE, CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(CAST(FECHA_EMPLAZAMIENTO AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_EMPLAZAMIENTO " +
            "FROM V3_TR_HUELGAJL " +
            "WHERE FECHA_EMPLAZAMIENTO <> DATE '1899-09-09' " +
            "  AND CAST(FECHA_EMPLAZAMIENTO AS DATE) < CAST(FECHA_APERTURA_EXPEDIENTE AS DATE)";

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("ENTIDAD_CLAVE"),
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE"),
                    rs.getString("FECHA_EMPLAZAMIENTO")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    public ArrayList<String[]> Fecha_AudienciaNE(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT ENTIDAD_CLAVE, CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(CAST(FECHA_AUDIENCIA AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_AUDIENCIA " +
            "FROM V3_TR_HUELGAJL " +
            "WHERE FECHA_AUDIENCIA <> DATE '1899-09-09' " +
            "  AND CAST(FECHA_AUDIENCIA AS DATE) <= CAST(FECHA_APERTURA_EXPEDIENTE AS DATE)";

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("ENTIDAD_CLAVE"),
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE"),
                    rs.getString("FECHA_AUDIENCIA")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    public ArrayList<String[]> Fecha_Acto_ProcesalNE(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT ENTIDAD_CLAVE, CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(CAST(FECHA_ACTO_PROCESAL AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_ACTO_PROCESAL " +
            "FROM V3_TR_HUELGAJL " +
            "WHERE FECHA_ACTO_PROCESAL <> DATE '1899-09-09' " +
            "  AND CAST(FECHA_ACTO_PROCESAL AS DATE) < CAST(FECHA_APERTURA_EXPEDIENTE AS DATE)";

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

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    public ArrayList<String[]> Fecha_Resolu_EmplazNE(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT ENTIDAD_CLAVE, CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(CAST(FECHA_RESOLU_EMPLAZ AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_RESOLU_EMPLAZ " +
            "FROM V3_TR_HUELGAJL " +
            "WHERE FECHA_RESOLU_EMPLAZ <> DATE '1899-09-09' " +
            "  AND CAST(FECHA_RESOLU_EMPLAZ AS DATE) < CAST(FECHA_APERTURA_EXPEDIENTE AS DATE)";

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("ENTIDAD_CLAVE"),
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE"),
                    rs.getString("FECHA_RESOLU_EMPLAZ")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    public ArrayList<String[]> Fecha_Resolu_HuelgaNE(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT ENTIDAD_CLAVE, CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(CAST(FECHA_RESOLU_HUELGA AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_RESOLU_HUELGA " +
            "FROM V3_TR_HUELGAJL " +
            "WHERE FECHA_RESOLU_HUELGA <> DATE '1899-09-09' " +
            "  AND CAST(FECHA_RESOLU_HUELGA AS DATE) < CAST(FECHA_APERTURA_EXPEDIENTE AS DATE)";

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("ENTIDAD_CLAVE"),
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE"),
                    rs.getString("FECHA_RESOLU_HUELGA")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    public ArrayList<String[]> Fecha_Estallam_HuelgaNE(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT ENTIDAD_CLAVE, CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(CAST(FECHA_ESTALLAM_HUELGA AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_ESTALLAM_HUELGA " +
            "FROM V3_TR_HUELGAJL " +
            "WHERE FECHA_ESTALLAM_HUELGA <> DATE '1899-09-09' " +
            "  AND CAST(FECHA_ESTALLAM_HUELGA AS DATE) <= CAST(FECHA_APERTURA_EXPEDIENTE AS DATE)";

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("ENTIDAD_CLAVE"),
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE"),
                    rs.getString("FECHA_ESTALLAM_HUELGA")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    public ArrayList<String[]> Fecha_Levant_HuelgaNE(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT ENTIDAD_CLAVE, CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(CAST(FECHA_LEVANT_HUELGA AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_LEVANT_HUELGA " +
            "FROM V3_TR_HUELGAJL " +
            "WHERE FECHA_LEVANT_HUELGA <> DATE '1899-09-09' " +
            "  AND CAST(FECHA_LEVANT_HUELGA AS DATE) <= CAST(FECHA_APERTURA_EXPEDIENTE AS DATE)";

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("ENTIDAD_CLAVE"),
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE"),
                    rs.getString("FECHA_LEVANT_HUELGA")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    // ----------------------------------------------------------
    // 8) Audiencias vs Huelga (SIN filtros)
    // ----------------------------------------------------------
    public ArrayList<String[]> Fecha_Aud_Presentacion(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT S.ENTIDAD_CLAVE, P.CLAVE_ORGANO, P.EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(CAST(P.FECHA_AUDIEN_CELEBRADA AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_AUDIEN_CELEBRADA, " +
            "       FORMATDATETIME(CAST(S.FECHA_PRESENTA_PETIC AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_PRESENTA_PETIC, " +
            "       P.ID_AUDIENCIA " +
            "FROM V3_TR_AUDIENCIASJL P " +
            "JOIN V3_TR_HUELGAJL S " +
            "  ON P.CLAVE_ORGANO = S.CLAVE_ORGANO " +
            " AND P.EXPEDIENTE_CLAVE = S.EXPEDIENTE_CLAVE " +
            "WHERE P.TIPO_PROCED = 4 " +
            "  AND P.FECHA_AUDIEN_CELEBRADA <> DATE '1899-09-09' " +
            "  AND S.FECHA_PRESENTA_PETIC <> DATE '1899-09-09' " +
            "  AND CAST(P.FECHA_AUDIEN_CELEBRADA AS DATE) < CAST(S.FECHA_PRESENTA_PETIC AS DATE)";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("ENTIDAD_CLAVE"),
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_AUDIEN_CELEBRADA"),
                    rs.getString("FECHA_PRESENTA_PETIC"),
                    rs.getString("ID_AUDIENCIA")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    public ArrayList<String[]> Fecha_Aud_Apertura(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT S.ENTIDAD_CLAVE, P.CLAVE_ORGANO, P.EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(CAST(P.FECHA_AUDIEN_CELEBRADA AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_AUDIEN_CELEBRADA, " +
            "       FORMATDATETIME(CAST(S.FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       P.ID_AUDIENCIA " +
            "FROM V3_TR_AUDIENCIASJL P " +
            "JOIN V3_TR_HUELGAJL S " +
            "  ON P.CLAVE_ORGANO = S.CLAVE_ORGANO " +
            " AND P.EXPEDIENTE_CLAVE = S.EXPEDIENTE_CLAVE " +
            "WHERE P.TIPO_PROCED = 4 " +
            "  AND P.FECHA_AUDIEN_CELEBRADA <> DATE '1899-09-09' " +
            "  AND S.FECHA_APERTURA_EXPEDIENTE <> DATE '1899-09-09' " +
            "  AND CAST(P.FECHA_AUDIEN_CELEBRADA AS DATE) < CAST(S.FECHA_APERTURA_EXPEDIENTE AS DATE)";

        System.out.println(sql);

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

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    // ----------------------------------------------------------
    // 9) Reglas de consistencia por fase (Huelga / Emplaz / Prehuelga) SIN filtros
    // ----------------------------------------------------------
    public ArrayList<String[]> Huelga(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       CASE WHEN ESTATUS_EXPEDIENTE = 1 THEN 'Solucionado' ELSE CAST(ESTATUS_EXPEDIENTE AS VARCHAR) END AS ESTATUS_EXPEDIENTE, " +
            "       CASE WHEN FASE_SOLI_EXPEDIENTE = 7 THEN 'Huelga' ELSE CAST(FASE_SOLI_EXPEDIENTE AS VARCHAR) END AS FASE_SOLI_EXPEDIENTE, " +
            "       CASE WHEN EMPLAZAMIENTO_HUELGA = 1 THEN 'SI' WHEN EMPLAZAMIENTO_HUELGA = 2 THEN 'NO' ELSE CAST(EMPLAZAMIENTO_HUELGA AS VARCHAR) END AS EMPLAZAMIENTO_HUELGA, " +
            "       CASE WHEN PREHUELGA = 1 THEN 'SI' WHEN PREHUELGA = 2 THEN 'NO' ELSE CAST(PREHUELGA AS VARCHAR) END AS PREHUELGA, " +
            "       CASE WHEN ESTALLAMIENTO_HUELGA = 1 THEN 'SI' WHEN ESTALLAMIENTO_HUELGA = 2 THEN 'NO' ELSE CAST(ESTALLAMIENTO_HUELGA AS VARCHAR) END AS ESTALLAMIENTO_HUELGA, " +
            "       FORMATDATETIME(CAST(FECHA_ESTALLAM_HUELGA AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_ESTALLAM_HUELGA, " +
            "       FORMATDATETIME(CAST(FECHA_EMPLAZAMIENTO AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_EMPLAZAMIENTO, " +
            "       FORMATDATETIME(CAST(FECHA_RESOLU_HUELGA AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_RESOLU_HUELGA, " +
            "       FORMATDATETIME(CAST(FECHA_LEVANT_HUELGA AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_LEVANT_HUELGA " +
            "FROM V3_TR_HUELGAJL " +
            "WHERE (FASE_SOLI_EXPEDIENTE = 7 AND ESTATUS_EXPEDIENTE = 1) " +
            "  AND ( " +
            "        EMPLAZAMIENTO_HUELGA <> 1 OR PREHUELGA <> 1 OR ESTALLAMIENTO_HUELGA <> 1 " +
            "        OR FECHA_RESOLU_HUELGA IS NULL OR FECHA_ESTALLAM_HUELGA IS NULL OR FECHA_LEVANT_HUELGA IS NULL " +
            "        OR FECHA_RESOLU_HUELGA = DATE '1899-09-09' " +
            "        OR FECHA_ESTALLAM_HUELGA = DATE '1899-09-09' " +
            "        OR FECHA_LEVANT_HUELGA = DATE '1899-09-09' " +
            "      )";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("ESTATUS_EXPEDIENTE"),
                    rs.getString("FASE_SOLI_EXPEDIENTE"),
                    rs.getString("EMPLAZAMIENTO_HUELGA"),
                    rs.getString("PREHUELGA"),
                    rs.getString("ESTALLAMIENTO_HUELGA"),
                    rs.getString("FECHA_ESTALLAM_HUELGA"),
                    rs.getString("FECHA_EMPLAZAMIENTO"),
                    rs.getString("FECHA_RESOLU_HUELGA"),
                    rs.getString("FECHA_LEVANT_HUELGA"),
                    //NOTA: MODIFICAR CLASE PRINCIPAL
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    public ArrayList<String[]> EMPLAZAMIENTO_HUELGA(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       CASE WHEN ESTATUS_EXPEDIENTE = 1 THEN 'Solucionado' ELSE CAST(ESTATUS_EXPEDIENTE AS VARCHAR) END AS ESTATUS_EXPEDIENTE, " +
            "       CASE WHEN FASE_SOLI_EXPEDIENTE = 5 THEN 'Emplazamiento a huelga' ELSE CAST(FASE_SOLI_EXPEDIENTE AS VARCHAR) END AS FASE_SOLI_EXPEDIENTE, " +
            "       CASE WHEN EMPLAZAMIENTO_HUELGA = 1 THEN 'SI' WHEN EMPLAZAMIENTO_HUELGA = 2 THEN 'NO' ELSE CAST(EMPLAZAMIENTO_HUELGA AS VARCHAR) END AS EMPLAZAMIENTO_HUELGA, " +
            "       CASE WHEN PREHUELGA = 1 THEN 'SI' WHEN PREHUELGA = 2 THEN 'NO' ELSE CAST(PREHUELGA AS VARCHAR) END AS PREHUELGA, " +
            "       CASE WHEN ESTALLAMIENTO_HUELGA = 1 THEN 'SI' WHEN ESTALLAMIENTO_HUELGA = 2 THEN 'NO' ELSE CAST(ESTALLAMIENTO_HUELGA AS VARCHAR) END AS ESTALLAMIENTO_HUELGA, " +
            "       FORMATDATETIME(CAST(FECHA_EMPLAZAMIENTO AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_EMPLAZAMIENTO, " +
            "       FORMATDATETIME(CAST(FECHA_RESOLU_EMPLAZ AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_RESOLU_EMPLAZ " +
            "FROM V3_TR_HUELGAJL " +
            "WHERE (FASE_SOLI_EXPEDIENTE = 5 AND ESTATUS_EXPEDIENTE = 1) " +
            "  AND (ESTALLAMIENTO_HUELGA <> 2 OR PREHUELGA <> 2)";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("ESTATUS_EXPEDIENTE"),
                    rs.getString("FASE_SOLI_EXPEDIENTE"),
                    rs.getString("EMPLAZAMIENTO_HUELGA"),
                    rs.getString("PREHUELGA"),
                    rs.getString("ESTALLAMIENTO_HUELGA"),
                    rs.getString("FECHA_EMPLAZAMIENTO"),
                    rs.getString("FECHA_RESOLU_EMPLAZ"),
                    //NOTA: MODIFICAR CLASE PRINCIPAL
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    public ArrayList<String[]> PREHUELGA(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       CASE WHEN ESTATUS_EXPEDIENTE = 1 THEN 'Solucionado' ELSE CAST(ESTATUS_EXPEDIENTE AS VARCHAR) END AS ESTATUS_EXPEDIENTE, " +
            "       CASE WHEN FASE_SOLI_EXPEDIENTE = 6 THEN 'Prehuelga' ELSE CAST(FASE_SOLI_EXPEDIENTE AS VARCHAR) END AS FASE_SOLI_EXPEDIENTE, " +
            "       CASE WHEN EMPLAZAMIENTO_HUELGA = 1 THEN 'SI' WHEN EMPLAZAMIENTO_HUELGA = 2 THEN 'NO' ELSE CAST(EMPLAZAMIENTO_HUELGA AS VARCHAR) END AS EMPLAZAMIENTO_HUELGA, " +
            "       CASE WHEN PREHUELGA = 1 THEN 'SI' WHEN PREHUELGA = 2 THEN 'NO' ELSE CAST(PREHUELGA AS VARCHAR) END AS PREHUELGA, " +
            "       CASE WHEN ESTALLAMIENTO_HUELGA = 1 THEN 'SI' WHEN ESTALLAMIENTO_HUELGA = 2 THEN 'NO' ELSE CAST(ESTALLAMIENTO_HUELGA AS VARCHAR) END AS ESTALLAMIENTO_HUELGA, " +
            "       FORMATDATETIME(CAST(FECHA_EMPLAZAMIENTO AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_EMPLAZAMIENTO, " +
            "       FORMATDATETIME(CAST(FECHA_RESOLU_EMPLAZ AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_RESOLU_EMPLAZ " +
            "FROM V3_TR_HUELGAJL " +
            "WHERE (FASE_SOLI_EXPEDIENTE = 6 AND ESTATUS_EXPEDIENTE = 1) " +
            "  AND (ESTALLAMIENTO_HUELGA <> 2 OR PREHUELGA <> 1 OR EMPLAZAMIENTO_HUELGA <> 1)";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("ESTATUS_EXPEDIENTE"),
                    rs.getString("FASE_SOLI_EXPEDIENTE"),
                    rs.getString("EMPLAZAMIENTO_HUELGA"),
                    rs.getString("PREHUELGA"),
                    rs.getString("ESTALLAMIENTO_HUELGA"),
                    rs.getString("FECHA_EMPLAZAMIENTO"),
                    rs.getString("FECHA_RESOLU_EMPLAZ"),
                    //NOTA: MODIFICAR CLASE PRINCIPAL
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    // ----------------------------------------------------------
    // 10) SinMotivo_Conflicto (SIN filtros)
    // ----------------------------------------------------------
    public ArrayList<String[]> SinMotivo_Conflicto(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE " +
            "FROM V3_TR_HUELGAJL " +
            "WHERE firma_contrato = 2 " +
            "  AND revision_contrato = 2 " +
            "  AND incumplim_contrato = 2 " +
            "  AND revision_salario = 2 " +
            "  AND reparto_utilidades = 2 " +
            "  AND apoyo_otra_huelga = 2 " +
            "  AND desequilibrio_fac_prod = 2 " +
            "  AND otro_motivo = 2";

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
}
