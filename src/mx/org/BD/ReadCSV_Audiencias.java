/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
CUANDO EL PROGRAMA SE QUEDA EN ST.EXECUTE POR CADA PROCEDIMIENTO FAVOR DE REVISAR DOCUMENTO YA QUE PUEDE SER LA TABLA
 TMP POR EL RAISE.APLICATION Y POR QUE LOS CAMPOS FECHA EN ,CSV NO ESTAN EN FORMATO FECHA
 */
package mx.org.BD;

import Screen_laborales.InsertaTR;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import javax.swing.JOptionPane;
import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;
import oracle.sql.STRUCT;
import oracle.sql.StructDescriptor;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import Bean_Procedures.Audiencias;
import Conexion.ConexionH2;
import ConverCat.Convers;
import Screen_laborales.cargando;
import java.io.BufferedInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;

/**
 *
 * @author ANTONIO.CORIA
 */
public class ReadCSV_Audiencias {

    public static String impErro = "", RutaT = "",CampoNotConver="";
    public static int TotalRegistros = 0;
    public static boolean borra_ruta = false;
    ArrayList Array;
    public static String rutaCarpetaArchivos = "";
    Convertir_utf8 conUTF8 = new Convertir_utf8();

    public void Read_Audiencias() throws FileNotFoundException, IOException {
        InsertaTR Insert = new InsertaTR();
        //FileInputStream f = new FileInputStream(Insert.rutaT);  

        try {
            System.out.println("1.....");
            if (Insert.CarpetaArchivos == true) {
                System.out.println("2.....");
                rutaCarpetaArchivos = Insert.rutaT + "CSV_BD_T.1.0_audiencias.csv";
                System.out.println("+++++2" + rutaCarpetaArchivos);
            } else {
                System.out.println("3.....");
                rutaCarpetaArchivos = Insert.rutaT;
                System.out.println("+++++3" + rutaCarpetaArchivos);
            }
            if (Insert.Bandera == false) {
                System.out.println("4.....");
                try ( BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(rutaCarpetaArchivos))) {
                    byte[] bytes = new byte[3];
                    int bytesRead = inputStream.read(bytes);

                    if (bytesRead >= 3 && bytes[0] == (byte) 0xEF && bytes[1] == (byte) 0xBB && bytes[2] == (byte) 0xBF) {
                        System.out.println("Archivo en UTF-8");
                        System.out.println("5.....");
                        IN_AUDIENCIAS(rutaCarpetaArchivos);
                    } else {
                        System.out.println("6.....");
                        conUTF8.Convertir_utf8_EBaseDatos(rutaCarpetaArchivos);
                        IN_AUDIENCIAS(conUTF8.rutaNuevoArchivo);
                        rutaCarpetaArchivos = conUTF8.rutaNuevoArchivo;

                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                System.out.println("7.....");
                try ( BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(rutaCarpetaArchivos))) {
                    byte[] bytes = new byte[3];
                    int bytesRead = inputStream.read(bytes);

                    if (bytesRead >= 3 && bytes[0] == (byte) 0xEF && bytes[1] == (byte) 0xBB && bytes[2] == (byte) 0xBF) {
                        System.out.println("8.....");
                        System.out.println("Archivo en UTF-8");
                        IN_AUDIENCIAS(rutaCarpetaArchivos);
                    } else {
                        System.out.println("9.....");
                        conUTF8.Convertir_utf8(rutaCarpetaArchivos);
                        IN_AUDIENCIAS(conUTF8.rutaNuevoArchivo);
                        rutaCarpetaArchivos = conUTF8.rutaNuevoArchivo;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.println("++++++" + rutaCarpetaArchivos);
            }
        } catch (Exception ex) {
        }

    }

    public void IN_AUDIENCIAS(String Ruta) throws Exception {

        String rutaArchivoCSV = Ruta;
        ArrayList<String[]> Array;
        Array = new ArrayList();
        ARRAY array_to_pass;
        CallableStatement st;
        Connection con = null;
        STRUCT[] structs;
        StructDescriptor sd;
        ArrayDescriptor descriptor;
        TotalRegistros = 0;
        boolean Inserta = true;
        Convers conver = new Convers();

        try ( BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(Ruta))) {
            byte[] bytes = new byte[3];
            int bytesRead = inputStream.read(bytes);

            if (bytesRead >= 3 && bytes[0] == (byte) 0xEF && bytes[1] == (byte) 0xBB && bytes[2] == (byte) 0xBF) {
                System.out.println("El archivo parece estar en UTF-8.");
                try ( BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(rutaArchivoCSV), StandardCharsets.UTF_8));  CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT)) {
                    int numeroColumnas = 0;
                    CSVRecord firstRecord = csvParser.iterator().next();
                    numeroColumnas = firstRecord.size();
                    System.out.println("numcol" + numeroColumnas);
                    if (numeroColumnas == 16) {
                        System.out.println("+hellooou+" + numeroColumnas);
                        cargando cargar = new cargando();

                        ArrayList<Audiencias> ad = new ArrayList<>();
                        for (CSVRecord record : csvParser) {
                           // System.out.println("llenado de csv");
                            TotalRegistros++;
                            Audiencias c = new Audiencias();
                            c.SetNOMBRE_ORGANO_JURIS(record.get(0).toUpperCase());
                            c.SetCLAVE_ORGANO(record.get(1).toUpperCase());
                            c.SetEXPEDIENTE_CLAVE(record.get(2).toUpperCase().replace("\\n", "").trim());
                            c.SetTIPO_PROCED(conver.CON_V3_TC_AUD_TIPO_PROCEJL(record.get(3).toUpperCase()));
                            c.SetID_AUDIENCIA(record.get(4).toUpperCase());
                            c.SetORDINARIO_TA(conver.CON_V3_TC_AUD_TIPO_AUDIENJL(record.get(5).toUpperCase()));
                            c.SetESPECIAL_INDIVI_TA(conver.CON_V3_TC_AUD_TIPO_AUDIENJL(record.get(6).toUpperCase()));
                            c.SetESPECIAL_COLECT_TA(conver.CON_V3_TC_AUD_TIPO_AUDIENJL(record.get(7).toUpperCase()));
                            c.SetHUELGA_TA(conver.CON_V3_TC_AUD_TIPO_AUDIENJL(record.get(8).toUpperCase()));
                            c.SetCOL_NATU_ECONOMICA_TA(conver.CON_V3_TC_AUD_TIPO_AUDIENJL(record.get(9).toUpperCase()));
                            c.SetESP_OTRO_AUDIENCIA(record.get(10).toUpperCase());
                            c.SetFECHA_AUDIEN_CELEBRADA(conver.toH2Date(record.get(11).toUpperCase(),"FECHA_AUDIEN"));
                            c.SetINICIO(record.get(12).toUpperCase());
                            c.SetCONCLU(record.get(13).toUpperCase());
                            c.SetCOMENTARIOS(record.get(14).toUpperCase());
                            //System.out.println(record.get(0) + ":" + record.get(1) + ":" + record.get(2) + ":" + record.get(3) + ":" + record.get(4) + ":" + record.get(5) + ":" + record.get(6) + ":" + record.get(7) + ":" + record.get(8) + ":" + record.get(9) + ":" + record.get(10) + ":" + record.get(11) + ":" + record.get(12) + ":" + record.get(13) + ":" + record.get(14) + ":" + Periodo);
                            ad.add(c);

                           /* System.out.println("NOMBRE_ORGANO_JURIS: " + record.get(0).toUpperCase());
                            System.out.println("CLAVE_ORGANO: " + record.get(1).toUpperCase());
                            System.out.println("EXPEDIENTE_CLAVE: " + record.get(2).toUpperCase().replace("\\n", "").trim());
                            System.out.println("TIPO_PROCED: " + conver.CON_V3_TC_AUD_TIPO_PROCEJL(record.get(3).toUpperCase()));
                            System.out.println("ID_AUDIENCIA: " + record.get(4).toUpperCase());
                            System.out.println("ORDINARIO_TA: " + conver.CON_V3_TC_AUD_TIPO_AUDIENJL(record.get(5).toUpperCase()));
                            System.out.println("ESPECIAL_INDIVI_TA: " + conver.CON_V3_TC_AUD_TIPO_AUDIENJL(record.get(6).toUpperCase()));
                            System.out.println("ESPECIAL_COLECT_TA: " + conver.CON_V3_TC_AUD_TIPO_AUDIENJL(record.get(7).toUpperCase()));
                            System.out.println("HUELGA_TA: " + conver.CON_V3_TC_AUD_TIPO_AUDIENJL(record.get(8).toUpperCase()));
                            System.out.println("COL_NATU_ECONOMICA_TA: " + conver.CON_V3_TC_AUD_TIPO_AUDIENJL(record.get(9).toUpperCase()));
                            System.out.println("ESP_OTRO_AUDIENCIA: " + record.get(10).toUpperCase());
                            System.out.println("FECHA_AUDIEN_CELEBRADA: " + conver.toH2Date(record.get(11).toUpperCase()));
                            System.out.println("INICIO: " + record.get(12).toUpperCase());
                            System.out.println("CONCLU: " + record.get(13).toUpperCase());
                            System.out.println("COMENTARIOS: " + record.get(14).toUpperCase());
                            System.out.println("PERIODO: " + Periodo);
                            System.out.println("--------------------------------------------------");*/

                            if (record.get(3).equals("1") && !(record.get(5).equals("") || record.get(5).equals("1") || record.get(5).equals("2") || record.get(5).equals("6") || record.get(5).equals("9"))) {

                                if (record.get(3).equals("1") && (record.get(5).equals("3"))) {
                                    c.SetORDINARIO_TA("6");
                                    c.SetESP_OTRO_AUDIENCIA("Audiencia de conciliación");
                                }
                                if (record.get(3).equals("1") && (record.get(5).equals("4"))) {
                                    c.SetORDINARIO_TA("6");
                                    c.SetESP_OTRO_AUDIENCIA("Audiencia conforme al artículo 937 (LFT)");
                                }
                                if (record.get(3).equals("1") && (record.get(5).equals("5"))) {
                                    c.SetORDINARIO_TA("6");
                                    c.SetESP_OTRO_AUDIENCIA("Audiencia dentro del procedimiento colectivo de naturaleza económica");
                                }

                                JOptionPane.showMessageDialog(null, "Error en el campo ORDINARIO_TA fuera de catalogo  Clave_organo:" + record.get(1) + " Expediente:" + record.get(2) + " Id_audiencia:" + record.get(4) + " nota: campo ORDINARIO_TA solo puede tener opcion 1,2,6 Y 9");

                                Inserta = false;
                            }
                            if (record.get(3).equals("2") && !(record.get(6).equals("") || record.get(6).equals("1") || record.get(6).equals("2") || record.get(6).equals("6") || record.get(6).equals("9"))) {
                                JOptionPane.showMessageDialog(null, "Error en el campo ESPECIAL_INDIVI_TA fuera de catalogo  Clave_organo:" + record.get(1) + " Expediente:" + record.get(2) + " Id_audiencia:" + record.get(4) + " nota: campo ESPECIAL_INDIVI_TA solo puede tener opcion 1,2,6 Y 9");
                                Inserta = false;
                            }
                            if (record.get(3).equals("3") && !(record.get(7).equals("") || record.get(7).equals("2") || record.get(7).equals("6") || record.get(7).equals("9"))) {
                                JOptionPane.showMessageDialog(null, "Error en el campo ESPECIAL_COLECT_TA fuera de catalogo  Clave_organo:" + record.get(1) + " Expediente:" + record.get(2) + " Id_audiencia:" + record.get(4) + " nota: campo ESPECIAL_COLECT_TA solo puede tener opcion 2,6 Y 9");
                                Inserta = false;
                            }
                            if (record.get(3).equals("4") && !(record.get(8).equals("") || record.get(8).equals("3") || record.get(8).equals("4") || record.get(8).equals("6") || record.get(8).equals("9"))) {
                                JOptionPane.showMessageDialog(null, "Error en el campo HUELGA_TA fuera de catalogo  Clave_organo:" + record.get(1) + " Expediente:" + record.get(2) + " Id_audiencia:" + record.get(4) + " nota: campo HUELGA_TA solo puede tener opcion 3,4,6 Y 9");
                                Inserta = false;
                            }
                            if (record.get(3).equals("5") && !(record.get(9).equals("") || record.get(9).equals("5") || record.get(9).equals("6") || record.get(9).equals("9"))) {
                                JOptionPane.showMessageDialog(null, "Error en el campo COL_NATU_ECONOMICA_TA fuera de catalogo  Clave_organo:" + record.get(1) + " Expediente:" + record.get(2) + " Id_audiencia:" + record.get(4) + " nota: campo COL_NATU_ECONOMICA_TA solo puede tener opcion 5,6 Y 9");
                                Inserta = false;
                            }
                        }

                        System.out.println("entro 1");

                        if (TotalRegistros > 0) {

                            if (Inserta == true) {
                                System.out.println("entro 2");
                                cargar.setVisible(true);
                                java.sql.PreparedStatement ps = null;

                                // OJO: AJUSTA los nombres reales de columnas en tu tabla H2
                                final String sql
                                        = "INSERT INTO V3_TR_AUDIENCIASJL ("
                                        + " NOMBRE_ORGANO_JURIS, CLAVE_ORGANO, EXPEDIENTE_CLAVE, TIPO_PROCED, ID_AUDIENCIA, "
                                        + " ORDINARIO_TA, ESPECIAL_INDIVI_TA, ESPECIAL_COLECT_TA, HUELGA_TA, COL_NATU_ECONOMICA_TA, "
                                        + " ESP_OTRO_AUDIENCIA, FECHA_AUDIEN_CELEBRADA, INICIO, CONCLU, COMENTARIOS"
                                        + ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

                                Connection conErr = null;

                                try {
                                    System.out.println("A) antes getConnection");
                                    con = ConexionH2.getConnection();
                                    System.out.println("B) despues getConnection");

                                    // ✅ Conexión separada para guardar ERRORES_INSERT (NO se revierte con rollback)
                                    conErr = ConexionH2.getConnection();
                                    conErr.setAutoCommit(true);

                                    con.setAutoCommit(false);

                                    System.out.println("C) antes prepareStatement");
                                    ps = con.prepareStatement(sql);
                                    System.out.println("D) despues prepareStatement");

                                    int batch = 0;
                                    int blockStart = 0; // índice inicial del bloque actual dentro de "ad"

                                    System.out.println("tamaño del array=" + ad.size());

                                    for (int i = 0; i < ad.size(); i++) {
                                        Audiencias a = ad.get(i);

                                        // ✅ usa tu helper para setear params (mismo orden de tu INSERT)
                                        setParamsAudiencias(ps, a);

                                        ps.addBatch();
                                        batch++;

                                        // Ejecutamos cada 1000
                                        if (batch % 1000 == 0) {
                                            int blockEnd = i + 1; // fin exclusivo

                                            try {
                                                ps.executeBatch();
                                                con.commit();
                                                System.out.println("✅ batch OK (bloque " + blockStart + " - " + blockEnd + ")");
                                                blockStart = blockEnd;
                                            } catch (BatchUpdateException bue) {
                                                System.err.println("❌ Batch falló (bloque " + blockStart + " - " + blockEnd + ")");
                                                System.err.println("Mensaje: " + bue.getMessage());
                                                con.rollback();

                                                // ✅ fallback: inserta uno por uno y guarda en ERRORES_INSERT lo que falle
                                                insertarBloqueUnoAUno(con, conErr, ps, ad, blockStart, blockEnd, "V3_TR_AUDIENCIASJL");

                                                // ✅ commit de los buenos (los que sí entraron en el uno-a-uno)
                                                con.commit();
                                                blockStart = blockEnd;
                                            }
                                        }
                                    }

                                    // ✅ Último bloque (si no fue múltiplo de 1000)
                                    if (blockStart < ad.size()) {
                                        try {
                                            ps.executeBatch();
                                            con.commit();
                                            System.out.println("✅ último batch OK (bloque " + blockStart + " - " + ad.size() + ")");
                                        } catch (BatchUpdateException bue) {
                                            System.err.println("❌ Último batch falló (bloque " + blockStart + " - " + ad.size() + ")");
                                            System.err.println("Mensaje: " + bue.getMessage());
                                            con.rollback();

                                            insertarBloqueUnoAUno(con, conErr, ps, ad, blockStart, ad.size(), "V3_TR_AUDIENCIASJL");
                                            con.commit();
                                        }
                                    }

                                    System.out.println("✅ Inserción terminada. Errores guardados en ERRORES_INSERT (si hubo).");

                                } catch (SQLException e) {
                                    System.out.println("error" + e);

                                    if (con != null) {
                                        con.rollback();
                                    }

                                    System.err.println("❌ SQLException");
                                    System.err.println("Mensaje: " + e.getMessage());
                                    System.err.println("SQLState: " + e.getSQLState());
                                    System.err.println("ErrorCode: " + e.getErrorCode());
                                    e.printStackTrace();

                                } catch (Exception e) {
                                    if (con != null) try {
                                        con.rollback();
                                    } catch (SQLException ex) {
                                    }
                                    System.err.println("❌ ERROR JAVA");
                                    e.printStackTrace();

                                } finally {
                                    if (ps != null) try {
                                        ps.close();
                                        System.out.println("parametros cerrados");
                                    } catch (SQLException ex) {
                                    }

                                    if (con != null) try {
                                        con.close();
                                        System.out.println("Conexion cerrada");
                                    } catch (SQLException ex) {
                                    }

                                    if (conErr != null) try {
                                        conErr.close();
                                        System.out.println("Conexion errores cerrada");
                                    } catch (SQLException ex) {
                                    }

                                    cargar.setVisible(false);
                                }
                            } else {
                            }

                        } else {
                            JOptionPane.showMessageDialog(null, "Archivo .CSV sin Registros-V3_TMP_AUDIENCIASJL");
                        }
                    } else {
                        JOptionPane.showMessageDialog(null, "Numero de columnas no coincide con la Base de datos");
                    }
                }
            } else {
                JOptionPane.showMessageDialog(null, "Gormato de archivo incorrecto");
            }
        }
    }

    private static final String SQL_INSERT_ERROR
            = "INSERT INTO ERRORES_INSERT "
            + "(TABLA_DESTINO, CLAVE_ORGANO, EXPEDIENTE_CLAVE, ID, "
            + " SQLSTATE, ERRORCODE, MENSAJE, REGISTRO_RAW) "
            + "VALUES (?,?,?,?,?,?,?,?)";

    private static void guardarError(Connection conErr, String tablaDestino, Audiencias a,
            SQLException e, String raw) {
        try ( PreparedStatement pe = conErr.prepareStatement(SQL_INSERT_ERROR)) {
            pe.setString(1, tablaDestino);
            pe.setString(2, a.GetCLAVE_ORGANO());
            pe.setString(3, a.GetEXPEDIENTE_CLAVE());
            pe.setString(4, a.GetID_AUDIENCIA());
            pe.setString(5, e.getSQLState());
            pe.setInt(6, e.getErrorCode());
            String msg = e.getMessage().replace("Violación de indice de Unicidad ó Clave primaria","Registro Duplicado");
            pe.setString(7, msg != null && msg.length() > 500 ? msg.substring(0, 250) : msg);
            pe.setString(8, raw);
           pe.executeUpdate();
        } catch (SQLException ex) {
            // Si hasta la tabla de errores falla, al menos lo imprimimos
            System.err.println("❌ No se pudo guardar en ERRORES_INSERT: " + ex.getMessage());
        }
    }

    private static void setParamsAudiencias(PreparedStatement ps, Audiencias a) throws SQLException {
        ps.setString(1, a.GetNOMBRE_ORGANO_JURIS());
        ps.setString(2, a.GetCLAVE_ORGANO());
        ps.setString(3, a.GetEXPEDIENTE_CLAVE());
        ps.setString(4, a.GetTIPO_PROCED());
        ps.setString(5, a.GetID_AUDIENCIA());

        ps.setString(6, a.GetORDINARIO_TA());
        ps.setString(7, a.GetESPECIAL_INDIVI_TA());
        ps.setString(8, a.GetESPECIAL_COLECT_TA());
        ps.setString(9, a.GetHUELGA_TA());
        ps.setString(10, a.GetCOL_NATU_ECONOMICA_TA());

        ps.setString(11, a.GetESP_OTRO_AUDIENCIA());
        ps.setString(12, a.GetFECHA_AUDIEN_CELEBRADA());
        ps.setString(13, a.GetINICIO());
        ps.setString(14, a.GetCONCLU());

        ps.setString(15, a.GetCOMENTARIOS());
    }

    private static void insertarBloqueUnoAUno(Connection con, Connection conErr, PreparedStatement ps,
            ArrayList<Audiencias> ad, int inicio, int fin, String tablaDestino) throws SQLException {

        for (int i = inicio; i < fin; i++) {
            Audiencias a = ad.get(i);

            try {
                setParamsAudiencias(ps, a);
                ps.executeUpdate(); // 👈 uno a uno
            } catch (SQLException e) {
                String raw = "idx=" + i
                        + "|CLAVE_ORGANO=" + a.GetCLAVE_ORGANO()
                        + "|EXPEDIENTE_CLAVE=" + a.GetEXPEDIENTE_CLAVE()
                        + "|ID_AUDIENCIA=" + a.GetID_AUDIENCIA()
                        + "|FECHA=" + a.GetFECHA_AUDIEN_CELEBRADA();

                guardarError(conErr, tablaDestino, a, e, raw);
            }
        }
    }

}
