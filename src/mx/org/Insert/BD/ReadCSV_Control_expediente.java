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
import Bean_Procedures.ControlExpediente;
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
public class ReadCSV_Control_expediente {

    public static String impErro = "", RutaT = "", CampoNotConver = "";
    public static int TotalRegistros = 0;
    public static boolean borra_ruta = false;
    ArrayList Array;
    public static String rutaCarpetaArchivos = "";
    Convertir_utf8 conUTF8 = new Convertir_utf8();

    public void Read_ControlExpediente(Connection con, Connection conErr) throws FileNotFoundException, IOException {
        InsertaTR Insert = new InsertaTR();
        //FileInputStream f = new FileInputStream(Insert.rutaT);  
        try {
            System.out.println("1.....");
            if (Insert.CarpetaArchivos == true) {
                System.out.println("2.....");
                rutaCarpetaArchivos = Insert.rutaT + "CSV_BD_Control_expediente.csv";
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
                        IN_CONTROL_EXPEDIENTE(rutaCarpetaArchivos, con, conErr);
                    } else {
                        System.out.println("6.....");
                        conUTF8.Convertir_utf8_EBaseDatos(rutaCarpetaArchivos);
                        IN_CONTROL_EXPEDIENTE(conUTF8.rutaNuevoArchivo, con, conErr);
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
                        IN_CONTROL_EXPEDIENTE(rutaCarpetaArchivos, con, conErr);
                    } else {
                        System.out.println("9.....");
                        conUTF8.Convertir_utf8(rutaCarpetaArchivos);
                        IN_CONTROL_EXPEDIENTE(conUTF8.rutaNuevoArchivo, con, conErr);
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

    public void IN_CONTROL_EXPEDIENTE(String Ruta, Connection con, Connection conErr) throws Exception {
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
                        ArrayList<ControlExpediente> ad = new ArrayList<>();
                        for (CSVRecord record : csvParser) {
                            System.out.println("llenado de csv");
                            TotalRegistros++;
                            ControlExpediente c = new ControlExpediente();
                            c.SetID_CONTROL(record.get(0).toUpperCase());
                            c.SetNOMBRE_ORGANO_JURIS(record.get(1).toUpperCase());
                            c.SetCLAVE_ORGANO(record.get(2).toUpperCase());
                            c.SetSEDE(record.get(3).toUpperCase());
                            c.SetJUECES_LABORAL_TOTAL(record.get(4).toUpperCase());
                            c.SetJUECES_LABORAL_SUB_HOM(record.get(5).toUpperCase());
                            c.SetJUECES_LABORAL_SUB_MUJ(record.get(6).toUpperCase());
                            c.SetJUECES_LABORAL_INDIV_HOM(record.get(7).toUpperCase());
                            c.SetJUECES_LABORAL_INDIV_MUJ(record.get(8).toUpperCase());
                            c.SetJUECES_LABORAL_COLEC_HOM(record.get(9).toUpperCase());
                            c.SetJUECES_LABORAL_COLEC_MUJ(record.get(10).toUpperCase());
                            c.SetJUECES_LABORAL_MIX_HOM(record.get(11).toUpperCase());
                            c.SetJUECES_LABORAL_MIX_MUJ(record.get(12).toUpperCase());
                            c.SetHORARIO(record.get(13).toUpperCase());
                            c.SetENTIDAD_CLAVE(record.get(14).toUpperCase());
                            c.SetENTIDAD_NOMBRE(record.get(15).toUpperCase());
                            if (!record.get(16).toUpperCase().equals(""))
                            {
                             if (record.get(16).toUpperCase().length()==1)
                             {
                            c.SetMUNICIPIO_CLAVE(record.get(14).toUpperCase()+"00"+record.get(16).toUpperCase());   
                             }
                             if (record.get(16).toUpperCase().length()==2)
                             {
                            c.SetMUNICIPIO_CLAVE(record.get(14).toUpperCase()+"0"+record.get(16).toUpperCase());     
                             }
                              if (record.get(16).toUpperCase().length() == 3) {
                                      c.SetMUNICIPIO_CLAVE(record.get(14).toUpperCase()+record.get(16).toUpperCase());
                                }
                             if (record.get(16).toUpperCase().length() == 5||record.get(13).toUpperCase().length()==4) {
                                      c.SetMUNICIPIO_CLAVE(record.get(14).toUpperCase());
                                }
                            }else{
                                 c.SetMUNICIPIO_CLAVE(record.get(16).toUpperCase()); 
                            }
                            c.SetMUNICIPIO_NOMBRE(record.get(17).toUpperCase());
                            c.SetCOLONIA_NOMBRE(record.get(18).toUpperCase());
                            c.SetLATITUD_ORG1(record.get(19).toUpperCase());
                            c.SetLONGITUD_ORG1(record.get(20).toUpperCase());
                            c.SetCIRCUNS_ORG_JUR(conver.CON_V3_TC_CIRCUNS_ORGANOJL(con, record.get(21).toUpperCase()));
                            c.SetOTRO_ESP_CIRCUNS(record.get(22).toUpperCase());
                            c.SetJURISDICCION(conver.CON_V3_TC_JURISDICCIONJL(con, record.get(23).toUpperCase()));
                            c.SetORDINARIO(record.get(24).toUpperCase());
                            c.SetESPECIAL_INDIVI(record.get(25).toUpperCase());
                            c.SetESPECIAL_COLECT(record.get(26).toUpperCase());
                            c.SetHUELGA(record.get(27).toUpperCase());
                            c.SetCOL_NATU_ECONOMICA(record.get(28).toUpperCase());
                            c.SetPARAP_VOLUNTARIO(record.get(29).toUpperCase());
                            c.SetTERCERIAS(record.get(30).toUpperCase());
                            c.SetPREF_CREDITO(record.get(31).toUpperCase());
                            c.SetEJECUCION(record.get(32).toUpperCase());
                            ad.add(c);
                            System.out.println("SUSHY");
                        }
                        System.out.println("entro 1");
                        if (TotalRegistros > 0) {
                            if (Inserta == true) {
                                System.out.println("entro 2");
                                cargar.setVisible(true);
                                // OJO: AJUSTA los nombres reales de columnas en tu tabla H2
                                final String sql
                                        = "INSERT INTO V3_TR_CONTROL_EXPEDIENTEJL ("
                                        + "ID_CONTROL, NOMBRE_ORGANO_JURIS, CLAVE_ORGANO, SEDE, JUECES_LABORAL_TOTAL,\n"
                                        + "JUECES_LABORAL_SUB_HOM, JUECES_LABORAL_SUB_MUJ, JUECES_LABORAL_INDIV_HOM, JUECES_LABORAL_INDIV_MUJ, JUECES_LABORAL_COLEC_HOM,\n"
                                        + "JUECES_LABORAL_COLEC_MUJ, JUECES_LABORAL_MIX_HOM, JUECES_LABORAL_MIX_MUJ, HORARIO, ENTIDAD_CLAVE,\n"
                                        + "ENTIDAD_NOMBRE, MUNICIPIO_CLAVE, MUNICIPIO_NOMBRE, COLONIA_NOMBRE, LATITUD_ORG,\n"
                                        + "LONGITUD_ORG, CIRCUNS_ORG_JUR, OTRO_ESP_CIRCUNS, JURISDICCION, ORDINARIO,\n"
                                        + "ESPECIAL_INDIVI, ESPECIAL_COLECT, HUELGA, COL_NATU_ECONOMICA, PARAP_VOLUNTARIO,\n"
                                        + "TERCERIAS, PREF_CREDITO, EJECUCION\n"
                                        + ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
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
                                        ControlExpediente a = ad.get(i);
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
                                                insertarBloqueUnoAUno(con, conErr, ps, ad, blockStart, blockEnd, "V3_TR_CONTROL_EXPEDIENTEJL");

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
                                            insertarBloqueUnoAUno(con, conErr, ps, ad, blockStart, ad.size(), "V3_TR_CONTROL_EXPEDIENTEJL");
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
                           // JOptionPane.showMessageDialog(null, "Archivo .CSV sin Registros-V3_TMP_AUDIENCIASJL");
                        }
                    } else {
                        JOptionPane.showMessageDialog(null, "Numero de columnas no coincide con la Base de datos");
                    }
                } catch (IOException e) {
                    JOptionPane.showMessageDialog(
                            null,
                            "Error al leer el archivo CSV:\n" + e.getMessage(),
                            "Error",
                            JOptionPane.ERROR_MESSAGE
                    );
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

    private static void guardarError(Connection conErr, String tablaDestino, ControlExpediente a,
            SQLException e, String raw) {
        try ( PreparedStatement pe = conErr.prepareStatement(SQL_INSERT_ERROR)) {
            pe.setString(1, tablaDestino);
            pe.setString(2, a.GetCLAVE_ORGANO());
            pe.setString(3, "");
            pe.setString(4, "");
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

    private static void setParamsAudiencias(PreparedStatement ps, ControlExpediente a) throws SQLException {
        ps.setString(1, a.GetID_CONTROL());
        ps.setString(2, a.GetNOMBRE_ORGANO_JURIS());
        ps.setString(3, a.GetCLAVE_ORGANO());
        ps.setString(4, a.GetSEDE());
        ps.setString(5, a.GetJUECES_LABORAL_TOTAL());

        ps.setString(6, a.GetJUECES_LABORAL_SUB_HOM());
        ps.setString(7, a.GetJUECES_LABORAL_SUB_MUJ());
        ps.setString(8, a.GetJUECES_LABORAL_INDIV_HOM());
        ps.setString(9, a.GetJUECES_LABORAL_INDIV_MUJ());
        ps.setString(10, a.GetJUECES_LABORAL_COLEC_HOM());

        ps.setString(11, a.GetJUECES_LABORAL_COLEC_MUJ());
        ps.setString(12, a.GetJUECES_LABORAL_MIX_HOM());
        ps.setString(13, a.GetJUECES_LABORAL_MIX_MUJ());
        ps.setString(14, a.GetHORARIO());
        ps.setString(15, a.GetENTIDAD_CLAVE());

        ps.setString(16, a.GetENTIDAD_NOMBRE());
        ps.setString(17, a.GetMUNICIPIO_CLAVE());
        ps.setString(18, a.GetMUNICIPIO_NOMBRE());
        ps.setString(19, a.GetCOLONIA_NOMBRE());
        ps.setString(20, a.GetLATITUD_ORG1());

        ps.setString(21, a.GetLONGITUD_ORG1());
        ps.setString(22, a.GetCIRCUNS_ORG_JUR());
        ps.setString(23, a.GetOTRO_ESP_CIRCUNS());
        ps.setString(24, a.GetJURISDICCION());
        ps.setString(25, a.GetORDINARIO());

        ps.setString(26, a.GetESPECIAL_INDIVI());
        ps.setString(27, a.GetESPECIAL_COLECT());
        ps.setString(28, a.GetHUELGA());
        ps.setString(29, a.GetCOL_NATU_ECONOMICA());
        ps.setString(30, a.GetPARAP_VOLUNTARIO());

        ps.setString(31, a.GetTERCERIAS());
        ps.setString(32, a.GetPREF_CREDITO());
        ps.setString(33, a.GetEJECUCION());
    }

    private static void insertarBloqueUnoAUno(Connection con, Connection conErr, PreparedStatement ps,
            ArrayList<ControlExpediente> ad, int inicio, int fin, String tablaDestino) throws SQLException {

        for (int i = inicio; i < fin; i++) {
            ControlExpediente a = ad.get(i);

            try {
                setParamsAudiencias(ps, a);
                ps.executeUpdate(); // 👈 uno a uno
            } catch (SQLException e) {
                String raw = "idx=" + i
                        + "|CLAVE_ORGANO=" + a.GetCLAVE_ORGANO()
                        + "|EXPEDIENTE_CLAVE=" + ""
                        + "|ID=" + "";
                       
                guardarError(conErr, tablaDestino, a, e, raw);
            }
        }
    }

}
