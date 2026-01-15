package mx.org.querys;

import Conexion.ConexionH2;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class V3QColectivo {

    private Connection getCon() throws SQLException {
        return ConexionH2.getConnection();
    }

    // ------------------------------------------------------------
    // Año_JudicialCampeche (H2 + sin filtros)
    // ------------------------------------------------------------
    public ArrayList<String[]> Año_JudicialCampeche(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, FECHA_APERTURA_EXPEDIENTES " +
            "FROM ( " +
            "  SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "         FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTES, " +
            "         RIGHT(FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd-MM-yyyy'), 4) AS FECHA_APERTURA_ANIO, " +
            "         (CAST(RIGHT(FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd-MM-yyyy'), 4) AS INT) + 1) AS FECHA_APERTURA_ANIO_MAS1, " +
            "         RIGHT(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(EXPEDIENTE_CLAVE, '-', ''), 'I', ''), 'J', ''), 'L', ''), '/', ''), 'll', ''), 'II', ''), 'I ', '')), 4) AS EXPE_ANIO " +
            "  FROM V3_TR_COLECTIVOJL " +
            ") X " +
            "WHERE X.FECHA_APERTURA_ANIO <> X.EXPE_ANIO " +
            // NOT BETWEEN 01/09/año AND 01/08/(año+1)
            "  AND CAST(PARSEDATETIME(X.FECHA_APERTURA_EXPEDIENTES, 'dd/MM/yyyy') AS DATE) " +
            "      NOT BETWEEN CAST(PARSEDATETIME('01/09/' || X.FECHA_APERTURA_ANIO, 'dd/MM/yyyy') AS DATE) " +
            "          AND CAST(PARSEDATETIME('01/08/' || X.FECHA_APERTURA_ANIO_MAS1, 'dd/MM/yyyy') AS DATE)";

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

    // ------------------------------------------------------------
    // Año_DIF_Campeche (H2 + sin filtros)
    // OJO: antes usabas PValidacion.AñoJuridico. Sin eso, dejo ejemplo fijo.
    // ------------------------------------------------------------
    public ArrayList<String[]> Año_DIF_Campeche(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        // Ajusta según tu lógica original
        String listaAnios = "('2020','2021','2022')";

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, FECHA_APERTURA_EXPEDIENTES " +
            "FROM ( " +
            "  SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "         FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTES, " +
            "         RIGHT(FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd-MM-yyyy'), 4) AS FECHA_APERTURA_ANIO, " +
            "         RIGHT(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(EXPEDIENTE_CLAVE, '-', ''), 'I', ''), 'J', ''), 'L', ''), '/', ''), 'll', ''), 'II', ''), 'I ', '')), 4) AS EXPE_ANIO " +
            "  FROM V3_TR_COLECTIVOJL " +
            ") X " +
            "WHERE X.FECHA_APERTURA_ANIO <> X.EXPE_ANIO " +
            "  AND X.EXPE_ANIO NOT IN " + listaAnios;

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

    // ------------------------------------------------------------
    // Año_Expe_ColectivoNE (H2 + sin filtros)
    // ------------------------------------------------------------
    public ArrayList<String[]> Año_Expe_ColectivoNE(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, FECHA_APERTURA_EXPEDIENTES " +
            "FROM ( " +
            "  SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "         FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTES, " +
            "         RIGHT(FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd-MM-yyyy'), 4) AS FECHA_APERTURA_ANIO, " +
            "         RIGHT(REGEXP_REPLACE(EXPEDIENTE_CLAVE, '[^0-9]', ''), 4) AS EXPE_ANIO " +
            "  FROM V3_TR_COLECTIVOJL " +
            ") X " +
            "WHERE X.FECHA_APERTURA_ANIO <> X.EXPE_ANIO " +
            "  AND X.EXPE_ANIO NOT IN ('2020','2021','2022','2023','2024','2025')";

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

    // ------------------------------------------------------------
    // FECHAS FUTURAS (H2 + sin filtros)
    // ------------------------------------------------------------
    public ArrayList<String[]> FECHA_APERTURA_EXPEDIENTE_FUT(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE,  " +
            "       FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE " +
            "FROM V3_TR_COLECTIVOJL " +
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

    public ArrayList<String[]> FECHA_PRES_DEMANDA_FUT(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE,  " +
            "       FORMATDATETIME(CAST(FECHA_PRES_DEMANDA AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_PRES_DEMANDA " +
            "FROM V3_TR_COLECTIVOJL " +
            "WHERE FECHA_PRES_DEMANDA > CURRENT_DATE " +
            "  AND FECHA_PRES_DEMANDA <> DATE '1899-09-09'";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    //NOTA:MODIFICAR CLASE PRINCIPAL
                    rs.getString("FECHA_PRES_DEMANDA")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return Array;
    }

    public ArrayList<String[]> FECHA_ADMI_DEMANDA_FUT(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE,  " +
            "       FORMATDATETIME(CAST(FECHA_ADMI_DEMANDA AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_ADMI_DEMANDA " +
            "FROM V3_TR_COLECTIVOJL " +
            "WHERE FECHA_ADMI_DEMANDA > CURRENT_DATE " +
            "  AND FECHA_ADMI_DEMANDA <> DATE '1899-09-09'";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    //NOTA:MODIFICAR CLASE PRINCIPAL
                    rs.getString("FECHA_ADMI_DEMANDA")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return Array;
    }

    public ArrayList<String[]> FECHA_DEPURACION_FUT(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE,  " +
            "       FORMATDATETIME(CAST(FECHA_DEPURACION AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_DEPURACION " +
            "FROM V3_TR_COLECTIVOJL " +
            "WHERE FECHA_DEPURACION > CURRENT_DATE " +
            "  AND FECHA_DEPURACION <> DATE '1899-09-09'";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    //NOTA:MODIFICAR CLASE PRINCIPAL
                    rs.getString("FECHA_DEPURACION")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return Array;
    }

    public ArrayList<String[]> FECHA_AUDIENCIA_JUICIO_FUT(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE,  " +
            "       FORMATDATETIME(CAST(FECHA_AUDIENCIA_JUICIO AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_AUDIENCIA_JUICIO " +
            "FROM V3_TR_COLECTIVOJL " +
            "WHERE FECHA_AUDIENCIA_JUICIO > CURRENT_DATE " +
            "  AND FECHA_AUDIENCIA_JUICIO <> DATE '1899-09-09'";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    //NOTA:MODIFICAR CLASE PRINCIPAL
                    rs.getString("FECHA_AUDIENCIA_JUICIO")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return Array;
    }

    public ArrayList<String[]> FECHA_DICTO_RESOLUCION_AD_FUT(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE,  " +
            "       FORMATDATETIME(CAST(FECHA_DICTO_RESOLUCION_AD AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_DICTO_RESOLUCION_AD " +
            "FROM V3_TR_COLECTIVOJL " +
            "WHERE FECHA_DICTO_RESOLUCION_AD > CURRENT_DATE " +
            "  AND FECHA_DICTO_RESOLUCION_AD <> DATE '1899-09-09'";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    //NOTA:MODIFICAR CLASE PRINCIPAL
                    rs.getString("FECHA_DICTO_RESOLUCION_AD")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return Array;
    }

    public ArrayList<String[]> FECHA_RESOLUCION_AJ_FUT(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE,  " +
            "       FORMATDATETIME(CAST(FECHA_RESOLUCION_AJ AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_RESOLUCION_AJ " +
            "FROM V3_TR_COLECTIVOJL " +
            "WHERE FECHA_RESOLUCION_AJ > CURRENT_DATE " +
            "  AND FECHA_RESOLUCION_AJ <> DATE '1899-09-09'";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    //NOTA:MODIFICAR CLASE PRINCIPAL
                    rs.getString("FECHA_RESOLUCION_AJ")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return Array;
    }

    // ------------------------------------------------------------
    // Duplicidad_expediente (H2 + sin filtros)
    // ------------------------------------------------------------
    public ArrayList<String[]> Duplicidad_expediente(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE " +
            "FROM V3_TR_COLECTIVOJL T " +
            "WHERE (CLAVE_ORGANO || REGEXP_REPLACE(EXPEDIENTE_CLAVE, '[^0-9]', '') ) IN ( " +
            "   SELECT (CLAVE_ORGANO || EXPEDIENTE_NUM ) FROM ( " +
            "      SELECT CLAVE_ORGANO,  REGEXP_REPLACE(EXPEDIENTE_CLAVE, '[^0-9]', '') AS EXPEDIENTE_NUM, COUNT(*) AS C " +
            "      FROM V3_TR_COLECTIVOJL " +
            "      GROUP BY CLAVE_ORGANO,  REGEXP_REPLACE(EXPEDIENTE_CLAVE, '[^0-9]', '') " +
            "   ) X WHERE X.C > 1 " +
            ") " +
            "ORDER BY CLAVE_ORGANO, REGEXP_REPLACE(EXPEDIENTE_CLAVE, '[^0-9]', '')";

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

    // ------------------------------------------------------------
    // Fecha_PresentacionNE (H2 + sin filtros)
    // Apertura < Presentación
    // ------------------------------------------------------------
    public ArrayList<String[]> Fecha_PresentacionNE(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT ENTIDAD_CLAVE, CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(CAST(FECHA_PRES_DEMANDA AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_PRES_DEMANDA " +
            "FROM V3_TR_COLECTIVOJL " +
            "WHERE FECHA_PRES_DEMANDA <> DATE '1899-09-09' " +
            "  AND CAST(FECHA_APERTURA_EXPEDIENTE AS DATE) < CAST(FECHA_PRES_DEMANDA AS DATE)";

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
                    rs.getString("FECHA_PRES_DEMANDA")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    // ------------------------------------------------------------
    // Fecha_PresentacionAdmiNE (H2 + sin filtros)
    // Admisión < Presentación
    // ------------------------------------------------------------
    public ArrayList<String[]> Fecha_PresentacionAdmiNE(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT ENTIDAD_CLAVE, CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(CAST(FECHA_ADMI_DEMANDA AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_ADMI_DEMANDA, " +
            "       FORMATDATETIME(CAST(FECHA_PRES_DEMANDA AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_PRES_DEMANDA " +
            "FROM V3_TR_COLECTIVOJL " +
            "WHERE FECHA_PRES_DEMANDA <> DATE '1899-09-09' " +
            "  AND FECHA_ADMI_DEMANDA <> DATE '1899-09-09' " +
            "  AND CAST(FECHA_ADMI_DEMANDA AS DATE) < CAST(FECHA_PRES_DEMANDA AS DATE)";

        System.out.println(sql);

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

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    // ------------------------------------------------------------
    // Fecha_Admi_DemandaNE (H2 + sin filtros)
    // Admisión < Apertura
    // ------------------------------------------------------------
    public ArrayList<String[]> Fecha_Admi_DemandaNE(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT ENTIDAD_CLAVE, CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(CAST(FECHA_ADMI_DEMANDA AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_ADMI_DEMANDA " +
            "FROM V3_TR_COLECTIVOJL " +
            "WHERE FECHA_ADMI_DEMANDA <> DATE '1899-09-09' " +
            "  AND CAST(FECHA_ADMI_DEMANDA AS DATE) < CAST(FECHA_APERTURA_EXPEDIENTE AS DATE)";

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
                    rs.getString("FECHA_ADMI_DEMANDA")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    // ------------------------------------------------------------
    // Fecha_Audiencia_JuicioNE (H2 + sin filtros)
    // Audiencia <= Apertura
    // ------------------------------------------------------------
    public ArrayList<String[]> Fecha_Audiencia_JuicioNE(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT ENTIDAD_CLAVE, CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(CAST(FECHA_AUDIENCIA_JUICIO AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_AUDIENCIA_JUICIO " +
            "FROM V3_TR_COLECTIVOJL " +
            "WHERE FECHA_AUDIENCIA_JUICIO <> DATE '1899-09-09' " +
            "  AND CAST(FECHA_AUDIENCIA_JUICIO AS DATE) <= CAST(FECHA_APERTURA_EXPEDIENTE AS DATE)";

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
                    rs.getString("FECHA_AUDIENCIA_JUICIO")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    // ------------------------------------------------------------
    // Fecha_DepuracionNE (H2 + sin filtros)
    // Depuración <= Apertura
    // ------------------------------------------------------------
    public ArrayList<String[]> Fecha_DepuracionNE(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT ENTIDAD_CLAVE, CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(CAST(FECHA_DEPURACION AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_DEPURACION " +
            "FROM V3_TR_COLECTIVOJL " +
            "WHERE FECHA_DEPURACION <> DATE '1899-09-09' " +
            "  AND CAST(FECHA_DEPURACION AS DATE) <= CAST(FECHA_APERTURA_EXPEDIENTE AS DATE)";

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
                    rs.getString("FECHA_DEPURACION")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    // ------------------------------------------------------------
    // Fecha_Acto_ProcesalNE (H2 + sin filtros)
    // Acto procesal < Apertura
    // ------------------------------------------------------------
    public ArrayList<String[]> Fecha_Acto_ProcesalNE(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT ENTIDAD_CLAVE, CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(CAST(FECHA_ACTO_PROCESAL AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_ACTO_PROCESAL " +
            "FROM V3_TR_COLECTIVOJL " +
            "WHERE FECHA_ACTO_PROCESAL <> DATE '1899-09-09' " +
            "  AND CAST(FECHA_ACTO_PROCESAL AS DATE) < CAST(FECHA_APERTURA_EXPEDIENTE AS DATE)";

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
                    rs.getString("FECHA_ACTO_PROCESAL")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    // ------------------------------------------------------------
    // Fecha_Resolucion_AjNE (H2 + sin filtros)
    // Resolución AJ < Apertura
    // ------------------------------------------------------------
    public ArrayList<String[]> Fecha_Resolucion_AjNE(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT ENTIDAD_CLAVE, CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(CAST(FECHA_RESOLUCION_AJ AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_RESOLUCION_AJ " +
            "FROM V3_TR_COLECTIVOJL " +
            "WHERE FECHA_RESOLUCION_AJ <> DATE '1899-09-09' " +
            "  AND CAST(FECHA_RESOLUCION_AJ AS DATE) < CAST(FECHA_APERTURA_EXPEDIENTE AS DATE)";

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
                    rs.getString("FECHA_RESOLUCION_AJ")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    // ------------------------------------------------------------
    // Fecha_Dicto_Resolucion_ADNE (H2 + sin filtros)
    // Dictó Resolución AD < Apertura
    // ------------------------------------------------------------
    public ArrayList<String[]> Fecha_Dicto_Resolucion_ADNE(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT ENTIDAD_CLAVE, CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(CAST(FECHA_APERTURA_EXPEDIENTE AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE, " +
            "       FORMATDATETIME(CAST(FECHA_DICTO_RESOLUCION_AD AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_DICTO_RESOLUCION_AD " +
            "FROM V3_TR_COLECTIVOJL " +
            "WHERE FECHA_DICTO_RESOLUCION_AD <> DATE '1899-09-09' " +
            "  AND CAST(FECHA_DICTO_RESOLUCION_AD AS DATE) < CAST(FECHA_APERTURA_EXPEDIENTE AS DATE)";

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
                    rs.getString("FECHA_DICTO_RESOLUCION_AD")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }

    // ------------------------------------------------------------
    // Fecha_Aud_Presentacion (JOIN con Audiencias) (H2 + sin filtros)
    // OJO: aquí tu Oracle decía P.TIPO_PROCED=3 (colectivo)
    // ------------------------------------------------------------
    public ArrayList<String[]> Fecha_Aud_Presentacion(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT S.ENTIDAD_CLAVE, P.CLAVE_ORGANO, P.EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(CAST(P.FECHA_AUDIEN_CELEBRADA AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_AUDIEN_CELEBRADA, " +
            "       FORMATDATETIME(CAST(S.FECHA_PRES_DEMANDA AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_PRES_DEMANDA, " +
            "       P.ID_AUDIENCIA " +
            "FROM V3_TR_AUDIENCIASJL P " +
            "JOIN V3_TR_COLECTIVOJL S " +
            "  ON P.CLAVE_ORGANO = S.CLAVE_ORGANO " +
            " AND P.EXPEDIENTE_CLAVE = S.EXPEDIENTE_CLAVE " +
            "WHERE P.TIPO_PROCED = '3' " +
            "  AND P.FECHA_AUDIEN_CELEBRADA <> DATE '1899-09-09' " +
            "  AND S.FECHA_PRES_DEMANDA <> DATE '1899-09-09' " +
            "  AND CAST(P.FECHA_AUDIEN_CELEBRADA AS DATE) < CAST(S.FECHA_PRES_DEMANDA AS DATE)";

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
                    rs.getString("FECHA_PRES_DEMANDA"),
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
            "JOIN V3_TR_COLECTIVOJL S " +
            "  ON P.CLAVE_ORGANO = S.CLAVE_ORGANO " +
            " AND P.EXPEDIENTE_CLAVE = S.EXPEDIENTE_CLAVE " +
            "WHERE P.TIPO_PROCED = '3' " +
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

    public ArrayList<String[]> Fecha_Aud_Admision(Connection con) {
        ArrayList<String[]> Array = new ArrayList<>();

        String sql =
            "SELECT S.ENTIDAD_CLAVE, P.CLAVE_ORGANO, P.EXPEDIENTE_CLAVE, " +
            "       FORMATDATETIME(CAST(P.FECHA_AUDIEN_CELEBRADA AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_AUDIEN_CELEBRADA, " +
            "       FORMATDATETIME(CAST(S.FECHA_ADMI_DEMANDA AS TIMESTAMP), 'dd/MM/yyyy') AS FECHA_ADMI_DEMANDA, " +
            "       P.ID_AUDIENCIA " +
            "FROM V3_TR_AUDIENCIASJL P " +
            "JOIN V3_TR_COLECTIVOJL S " +
            "  ON P.CLAVE_ORGANO = S.CLAVE_ORGANO " +
            " AND P.EXPEDIENTE_CLAVE = S.EXPEDIENTE_CLAVE " +
            "WHERE P.TIPO_PROCED = '3' " +
            "  AND P.FECHA_AUDIEN_CELEBRADA <> DATE '1899-09-09' " +
            "  AND S.FECHA_ADMI_DEMANDA <> DATE '1899-09-09' " +
            "  AND CAST(P.FECHA_AUDIEN_CELEBRADA AS DATE) < CAST(S.FECHA_ADMI_DEMANDA AS DATE)";

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
                    rs.getString("FECHA_ADMI_DEMANDA"),
                    rs.getString("ID_AUDIENCIA")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Array;
    }
}
