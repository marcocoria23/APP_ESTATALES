/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
CUANDO EL PROGRAMA SE QUEDA EN ST.EXECUTE POR CADA PROCEDIMIENTO FAVOR DE REVISAR DOCUMENTO YA QUE PUEDE SER LA TABLA
 TMP POR EL RAISE.APLICATION Y POR QUE LOS CAMPOS FECHA EN ,CSV NO ESTAN EN FORMATO FECHA
 */
package mx.org.Insert.BD;

import Pantallas_laborales.InsertaTR;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import javax.swing.JOptionPane;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import Bean_Procedures.Part_Dem_Colectivo;
import ConverCat.Convers;
import Pantallas_laborales.cargando;
import java.io.BufferedInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;

/**
 *
 * @author ANDREA.HERNANDEZL
 */
public class ReadCSV_Part_Dem_Colectivo {

    public static String impErro = "", RutaT = "", CampoNotConver = "";
    public static int TotalRegistros = 0;
    public static boolean borra_ruta = false;
    ArrayList Array;
    public static String rutaCarpetaArchivos = "";
    Convertir_utf8 conUTF8 = new Convertir_utf8();

    public void Read_Part_Dem_Colectivo(Connection con, Connection conErr) throws FileNotFoundException, IOException {
        InsertaTR Insert = new InsertaTR();
        //FileInputStream f = new FileInputStream(Insert.rutaT);  
        try {
            System.out.println("1.....");
            if (Insert.CarpetaArchivos == true) {
                System.out.println("2.....");
                rutaCarpetaArchivos = Insert.rutaT + "CSV_BD_T.3.3_part_dem_esp_colec.csv";
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
                        IN_PART_DEM_COLECTIVO(rutaCarpetaArchivos, con, conErr);
                    } else {
                        System.out.println("6.....");
                        conUTF8.Convertir_utf8_EBaseDatos(rutaCarpetaArchivos);
                        IN_PART_DEM_COLECTIVO(conUTF8.rutaNuevoArchivo, con, conErr);
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
                        IN_PART_DEM_COLECTIVO(rutaCarpetaArchivos, con, conErr);
                    } else {
                        System.out.println("9.....");
                        conUTF8.Convertir_utf8(rutaCarpetaArchivos);
                        IN_PART_DEM_COLECTIVO(conUTF8.rutaNuevoArchivo, con, conErr);
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

    public void IN_PART_DEM_COLECTIVO(String Ruta, Connection con, Connection conErr) throws Exception {
        String rutaArchivoCSV = Ruta;
        Array = new ArrayList();
        TotalRegistros = 0;
        boolean Inserta = true;
        Convers conver = new Convers();
        java.sql.PreparedStatement ps = null;

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
                    if (numeroColumnas == 33) {
                        System.out.println("+hellooou+" + numeroColumnas);
                        cargando cargar = new cargando();
                        ArrayList<Part_Dem_Colectivo> ad = new ArrayList<>();
                        for (CSVRecord record : csvParser) {
                            // System.out.println("llenado de csv");
                            TotalRegistros++;
                            Part_Dem_Colectivo c = new Part_Dem_Colectivo();
                            c.SetNOMBRE_ORGANO_JURIS(record.get(0).toUpperCase());
                            c.SetCLAVE_ORGANO(record.get(1).toUpperCase());
                            c.SetEXPEDIENTE_CLAVE(record.get(2).toUpperCase().replace("\\n", "").trim());
                            c.SetID_DEMANDADO(record.get(3).toUpperCase());
                            c.SetDEMANDADO(conver.CON_V3_TC_DEMANDADOJL(con, record.get(4).toUpperCase()));
                            c.SetDEFENSA_DEM(conver.CON_V3_TC_TIPO_DEFENSAJL(con, record.get(5).toUpperCase()));
                            c.SetNOMBRE_SINDICATO_DEM(record.get(6).toUpperCase());
                            c.SetREG_ASOC_SINDICAL_DEM(record.get(7).toUpperCase());
                            c.SetTIPO_SINDICATO_DEM(conver.CON_V3_TC_TIPO_SINDICATOJL(con, record.get(8).toUpperCase()));
                            c.SetOTRO_ESP_SINDICATO_DEM(record.get(9).toUpperCase());
                            c.SetORG_OBRERA_DEM(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(10).toUpperCase()));
                            c.SetNOMBRE_ORG_OBRERA_DEM(conver.CON_V3_TC_ORGAN_OBRERAJL(con, record.get(11).toUpperCase()));
                            c.SetOTRO_ESP_OBRERA_DEM(record.get(12).toUpperCase());
                            c.SetCANT_TRABAJA_INV_DEM(record.get(13).toUpperCase());
                            c.SetHOMBRES_DEM(record.get(14).toUpperCase());
                            c.SetMUJERES_DEM(record.get(15).toUpperCase());
                            c.SetNO_IDENTIFICADO_DEM(record.get(16).toUpperCase());
                            c.SetTIPO_DEM_PAT(conver.CON_V3_TC_TIPO_PATRONJL(con, record.get(17).toUpperCase()));
                            c.SetRFC_PATRON_DEM(record.get(18).toUpperCase());
                            c.SetRAZON_SOCIAL_EMPR_DEM(record.get(19).toUpperCase());
                            c.SetCALLE(record.get(20).toUpperCase());
                            c.SetN_EXT(record.get(21).toUpperCase());
                            c.SetN_INT(record.get(22).toUpperCase());
                            c.SetCOLONIA(record.get(23).toUpperCase());
                            c.SetCP(record.get(24).toUpperCase());
                            c.SetENTIDAD_NOMBRE_EMPR(record.get(25).toUpperCase());
                            c.SetENTIDAD_CLAVE_EMPR(record.get(26).toUpperCase());
                            c.SetMUNICIPIO_NOMBRE_EMPR(record.get(27).toUpperCase());
                            c.SetMUNICIPIO_CLAVE_EMPR(record.get(28).toUpperCase());
                            if (!record.get(28).toUpperCase().equals("")) {
                                if (record.get(28).toUpperCase().length() == 1) {
                                    c.SetMUNICIPIO_CLAVE_EMPR(record.get(26).toUpperCase() + "00" + record.get(28).toUpperCase());
                                }
                                if (record.get(28).toUpperCase().length() == 2) {
                                    c.SetMUNICIPIO_CLAVE_EMPR(record.get(26).toUpperCase() + "0" + record.get(28).toUpperCase());
                                }
                                if (record.get(28).toUpperCase().length() == 5) {
                                    c.SetMUNICIPIO_CLAVE_EMPR(record.get(28).toUpperCase());
                                }
                                if (record.get(28).toUpperCase().length() == 3) {
                                      c.SetMUNICIPIO_CLAVE_EMPR(record.get(26).toUpperCase()+record.get(28).toUpperCase());
                                }
                            } else {
                                c.SetMUNICIPIO_CLAVE_EMPR(record.get(28).toUpperCase());
                            }
                            c.SetLATITUD1_EMPR(record.get(29).toUpperCase());
                            c.SetLONGITUD1_EMPR(record.get(30).toUpperCase());
                            c.SetCOMENTARIOS(record.get(31).toUpperCase());
                            ad.add(c);
                        }
                        System.out.println("entro 1");
                        if (TotalRegistros > 0) {
                            if (Inserta == true) {
                                System.out.println("entro 2");
                                cargar.setVisible(true);
                                // OJO: AJUSTA los nombres reales de columnas en tu tabla H2
                                final String sql
                                        = "INSERT INTO V3_TR_PART_DEM_COLECTIVOJL ("
                                        + "NOMBRE_ORGANO_JURIS, CLAVE_ORGANO, EXPEDIENTE_CLAVE, ID_DEMANDADO, DEMANDADO,\n"
                                        + "DEFENSA_DEM, NOMBRE_SINDICATO_DEM, REG_ASOC_SINDICAL_DEM, TIPO_SINDICATO_DEM, OTRO_ESP_SINDICATO_DEM,\n"
                                        + "ORG_OBRERA_DEM, NOMBRE_ORG_OBRERA_DEM, OTRO_ESP_OBRERA_DEM, CANT_TRABAJA_INV_DEM, HOMBRES_DEM,\n"
                                        + "MUJERES_DEM, NO_IDENTIFICADO_DEM, TIPO_DEM_PAT, RFC_PATRON_DEM, RAZON_SOCIAL_EMPR_DEM,\n"
                                        + "CALLE, N_EXT, N_INT, COLONIA, CP,\n"
                                        + "ENTIDAD_NOMBRE_EMPR, ENTIDAD_CLAVE_EMPR, MUNICIPIO_NOMBRE_EMPR, MUNICIPIO_CLAVE_EMPR, LATITUD_EMPR,\n"
                                        + "LONGITUD_EMPR, COMENTARIOS"
                                        + ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
                                try {
                                    System.out.println("A) antes getConnection");
                                    System.out.println("B) despues getConnection");
                                    // ✅ Conexión separada para guardar ERRORES_INSERT (NO se revierte con rollback)
                                    conErr.setAutoCommit(true);
                                    con.setAutoCommit(false);
                                    System.out.println("C) antes prepareStatement");
                                    ps = con.prepareStatement(sql);
                                    System.out.println("D) despues prepareStatement");
                                    int batch = 0;
                                    int blockStart = 0; // índice inicial del bloque actual dentro de "ad"
                                    System.out.println("tamaño del array=" + ad.size());
                                    for (int i = 0; i < ad.size(); i++) {
                                        Part_Dem_Colectivo a = ad.get(i);
                                        // ✅ usa tu helper para setear params (mismo orden de tu INSERT)
                                        setParamsPart_Dem_Colectivo(ps, a);
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
                                                insertarBloqueUnoAUno(con, conErr, ps, ad, blockStart, blockEnd, "V3_TR_PART_DEM_COLECTIVOJL");

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
                                            insertarBloqueUnoAUno(con, conErr, ps, ad, blockStart, ad.size(), "V3_TR_PART_DEM_COLECTIVOJL");
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
                                    cargar.setVisible(false);
                                }
                            } else {
                            }

                        } else {
                            JOptionPane.showMessageDialog(null, "Archivo .CSV sin Registros-V3_TMP_PART_DEM_COLECTIVOJL");
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

    private static void guardarError(Connection conErr, String tablaDestino, Part_Dem_Colectivo a,
            SQLException e, String raw) {
        try ( PreparedStatement pe = conErr.prepareStatement(SQL_INSERT_ERROR)) {
            pe.setString(1, tablaDestino);
            pe.setString(2, a.GetCLAVE_ORGANO());
            pe.setString(3, a.GetEXPEDIENTE_CLAVE());
            pe.setString(4, a.GetID_DEMANDADO());
            pe.setString(5, e.getSQLState());
            pe.setInt(6, e.getErrorCode());
             String msg = e.getMessage().replace("Violación de indice de Unicidad ó Clave primaria", "Registro Duplicado").replace("Violación de una restricción de Integridad Referencial", "Valor de Catalogo no encontrado");
            pe.setString(7, msg != null && msg.length() > 500 ? msg.substring(0, 250) : msg);
            pe.setString(8, raw);
            pe.executeUpdate();
        } catch (SQLException ex) {
            // Si hasta la tabla de errores falla, al menos lo imprimimos
            System.err.println("❌ No se pudo guardar en ERRORES_INSERT: " + ex.getMessage());
        }
    }

    private static void setParamsPart_Dem_Colectivo(PreparedStatement ps, Part_Dem_Colectivo a) throws SQLException {
        ps.setString(1, a.GetNOMBRE_ORGANO_JURIS());
        ps.setString(2, a.GetCLAVE_ORGANO());
        ps.setString(3, a.GetEXPEDIENTE_CLAVE());
        ps.setString(4, a.GetID_DEMANDADO());
        ps.setString(5, a.GetDEMANDADO());

        ps.setString(6, a.GetDEFENSA_DEM());
        ps.setString(7, a.GetNOMBRE_SINDICATO_DEM());
        ps.setString(8, a.GetREG_ASOC_SINDICAL_DEM());
        ps.setString(9, a.GetTIPO_SINDICATO_DEM());
        ps.setString(10, a.GetOTRO_ESP_SINDICATO_DEM());

        ps.setString(11, a.GetORG_OBRERA_DEM());
        ps.setString(12, a.GetNOMBRE_ORG_OBRERA_DEM());
        ps.setString(13, a.GetOTRO_ESP_OBRERA_DEM());
        ps.setString(14, a.GetCANT_TRABAJA_INV_DEM());
        ps.setString(15, a.GetHOMBRES_DEM());

        ps.setString(16, a.GetMUJERES_DEM());
        ps.setString(17, a.GetNO_IDENTIFICADO_DEM());
        ps.setString(18, a.GetTIPO_DEM_PAT());
        ps.setString(19, a.GetRFC_PATRON_DEM());
        ps.setString(20, a.GetRAZON_SOCIAL_EMPR_DEM());

        ps.setString(21, a.GetCALLE());
        ps.setString(22, a.GetN_EXT());
        ps.setString(23, a.GetN_INT());
        ps.setString(24, a.GetCOLONIA());
        ps.setString(25, a.GetCP());

        ps.setString(26, a.GetENTIDAD_NOMBRE_EMPR());
        ps.setString(27, a.GetENTIDAD_CLAVE_EMPR());
        ps.setString(28, a.GetMUNICIPIO_NOMBRE_EMPR());
        ps.setString(29, a.GetMUNICIPIO_CLAVE_EMPR());
        ps.setString(30, a.GetLATITUD1_EMPR());

        ps.setString(31, a.GetLONGITUD1_EMPR());
        ps.setString(32, a.GetCOMENTARIOS());

    }

    private static void insertarBloqueUnoAUno(Connection con, Connection conErr, PreparedStatement ps,
            ArrayList<Part_Dem_Colectivo> ad, int inicio, int fin, String tablaDestino) throws SQLException {

        for (int i = inicio; i < fin; i++) {
            Part_Dem_Colectivo a = ad.get(i);

            try {
                setParamsPart_Dem_Colectivo(ps, a);
                ps.executeUpdate(); // 👈 uno a uno
            } catch (SQLException e) {
                String raw = "idx=" + i
                        + "|CLAVE_ORGANO=" + a.GetCLAVE_ORGANO()
                        + "|EXPEDIENTE_CLAVE=" + a.GetEXPEDIENTE_CLAVE()
                        + "|ID_AUDIENCIA=" + a.GetID_DEMANDADO();
                guardarError(conErr, tablaDestino, a, e, raw);
            }
        }
    }

}
