/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mx.org.Insert.BD;

import Pantallas_laborales.InsertaTR;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

/**
 *
 * @author ANTONIO.CORIA
 */
public class Convertir_utf8 {

    public static String rutaNuevoArchivo = "";

    public void Convertir_utf8(String Ruta) {
        String rutaArchivo = Ruta;
        // Ruta del nuevo archivo UTF-8
        rutaNuevoArchivo = Ruta.replace(".csv", "UTF8.csv");
        InsertaTR Insert = new InsertaTR();
        int Fila0 = 0, Fila01 = 0;
        List<Integer> columnasAEliminar = new ArrayList<>();
        List<String> Prin = new ArrayList<>();
        Prin.add("V3_TMP_ORDINARIOJL");
        Prin.add("V3_TMP_INDIVIDUALJL");
        Prin.add("V3_TMP_COLECTIVOJL");
        Prin.add("V3_TMP_HUELGAJL");
        Prin.add("V3_TMP_COLECTIVOJL");
        Prin.add("V3_TMP_COLECT_ECONOMJL");
        Prin.add("V3_TMP_TERCERIASJL");
        Prin.add("V3_TMP_PREF_CREDITOJL");
        Prin.add("V3_TMP_PARAPROCESALJL");
        Prin.add("V3_TMP_EJECUCIONJL");
        Prin.add("V3_TMP_PART_ACT_ORDINARIOJL");
        Prin.add("V3_TMP_PART_ACT_INDIVIDUALJL");
        Prin.add("V3_TMP_PART_ACT_HUELGAJL");
        Prin.add("V3_TMP_PART_ACT_COLECT_ECONOMJL");

        List<String> Sec = new ArrayList<>();
        Sec.add("V3_TMP_PART_DEM_ORDINARIOJL");
        Sec.add("V3_TMP_PART_DEM_INDIVIDUALJL");
        Sec.add("V3_TMP_PART_DEM_HUELGAJL");

        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(
                    new FileInputStream(rutaArchivo), "Cp1252")); // ANSI (Windows-1252)
            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT);
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(rutaNuevoArchivo), "UTF-8"));
            writer.write("\uFEFF");

            CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT);
             columnasAEliminar.clear();
            // Leer y escribir línea por línea
            if (Insert.tabla.equals("V3_TMP_CONTROL_EXPEDIENTEJL")) {
                Fila0 = 9;
                Fila01 = 11;
                columnasAEliminar.add(20);
                 System.out.println("mas1");
            }
            if (Insert.tabla.equals("V3_TMP_AUDIENCIASJL")) {
                Fila0 = 9;
                Fila01 = 11;
                columnasAEliminar.add(0);
                System.out.println("mas2");
            }
            if (Prin.contains(Insert.tabla)) {
                Fila0 = 9;
                Fila01 = 11;
                columnasAEliminar.add(0);
                System.out.println("mas3");
            }
            if (Sec.contains(Insert.tabla)) {
                Fila0 = 9;
                Fila01 = 11;
                columnasAEliminar.add(0);
                columnasAEliminar.add(20);
                System.out.println("mas4");
            }
            if (Insert.tabla.equals("V3_TMP_PART_ACT_COLECTIVOJL")
                    || Insert.tabla.equals("V3_TMP_PART_DEM_COLECTIVOJL")
                    || Insert.tabla.equals("V3_TMP_PART_ACT_COLECT_ECONOMJL")
                    || Insert.tabla.equals("V3_TMP_PART_DEM_COLECT_ECONOMJL")) {
                Fila0 = 9;
                Fila01 = 11;
                columnasAEliminar.add(0);
                columnasAEliminar.add(31);
                System.out.println("mas5");
            }

            int contadorLineas = 0;
            for (CSVRecord record : csvParser) {
                contadorLineas++;
                if (contadorLineas > Fila0 && contadorLineas != Fila01) {
                    // Construir una nueva línea excluyendo las columnas especificadas
                    List<String> nuevaLinea = new ArrayList<>();
                    System.out.println("record.size()"+record.size());
                    for (int i = 0; i < record.size(); i++) {
                        if (!columnasAEliminar.contains(i)) {
                            nuevaLinea.add(record.get(i));
                        }
                    }
                    // Imprimir la nueva línea en el archivo de salida
                    csvPrinter.printRecord(nuevaLinea);
                }
            }

            // Cerrar los recursos
            reader.close();
            csvPrinter.close();

            System.out.println("Archivo convertido a UTF-8 con éxito.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void Convertir_utf8_EBaseDatos(String Ruta) {
        String rutaArchivo = Ruta;
        // Ruta del nuevo archivo UTF-8
        rutaNuevoArchivo = Ruta.replace(".csv", "UTF8.csv");
        InsertaTR Insert = new InsertaTR();
       
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(
                    new FileInputStream(rutaArchivo), "Cp1252")); // ANSI (Windows-1252)
            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT);
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(rutaNuevoArchivo), "UTF-8"));
            writer.write("\uFEFF");
            CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT);
            // Leer y escribir línea por línea
            //int contadorLineas = 0;
            for (CSVRecord record : csvParser) {
              //  contadorLineas++;
                    // Construir una nueva línea excluyendo las columnas especificadas
                    List<String> nuevaLinea = new ArrayList<>();
                    for (int i = 0; i < record.size(); i++) {
                            nuevaLinea.add(record.get(i));
                    }
                    // Imprimir la nueva línea en el archivo de salida
                    csvPrinter.printRecord(nuevaLinea);
                }
            // Cerrar los recursos
            reader.close();
            csvPrinter.close();
            System.out.println("Archivo convertido a BD UTF-8 CON EXITO.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
