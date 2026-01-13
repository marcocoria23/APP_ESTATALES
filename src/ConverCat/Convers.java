/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package ConverCat;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 *
 * @author ANTONIO.CORIA
 */
public class Convers {

    DateTimeFormatter F_DMY = DateTimeFormatter.ofPattern("dd/MM/yyyy");
    DateTimeFormatter F_ISO = DateTimeFormatter.ISO_LOCAL_DATE;


   public String toH2Date(String s,String Campo) {

    try {
        if (s == null) {
            return null;
        }

        s = s.trim();
        if (s.isEmpty() || s.equalsIgnoreCase("NULL")) {
            return null;
        }

        // Normaliza separador
        s = s.replace('-', '/');

        // Intenta convertir dd/MM/yyyy
        LocalDate d = LocalDate.parse(s, F_DMY);

        // Devuelve yyyy-MM-dd (H2)
        return d.format(F_ISO);

    } catch (Exception e) {
        return "ERROR EN FORMATO FECHA CAMPO:||"+Campo;
    }
}


    public String CON_V3_TC_AUD_TIPO_PROCEJL(String campo) {
        if (!campo.equals("")) {
            switch (campo.toUpperCase().trim()) {
                case "ORDINARIO":
                    return "1";

                case "ESPECIAL INDIVIDUAL":
                    return "2";

                case "ESPECIAL COLECTIVO":
                    return "3";

                case "HUELGA":
                    return "4";

                case "COLECTIVO DE NATURALEZA ECONOMICA":
                    return "5";

                case "NO IDENTIFICADO":
                    return "9";

                default:
                    return "Valor Cat No encontrado";
            }
        } else {
            return null;
        }
    }

    public String CON_V3_TC_AUD_TIPO_AUDIENJL(String campo) {
     if (!campo.equals("")) {    
        switch (campo.toUpperCase().trim()) {
            case "AUDIENCIA PRELIMINAR":
                return "1";

            case "AUDIENCIA DE JUICIO":
                return "2";

            case "AUDIENCIA DE CONCILIACION":
                return "3";             
                 case "AUDIENCIA DE CONCILIACIÓN":
                return "3";
            case "AUDIENCIA CONFORME AL ARTICULO 937 (LFT)":
                return "4";

            case "AUDIENCIA DENTRO DEL PROCEDIMIENTO COLECTIVO DE NATURALEZA ECONOMICA":
                return "5";

            case "OTRO TIPO DE AUDIENCIA (ESPECIFIQUE)":
                return "6";
            case "NO IDENTIFICADA":
                return "9";
       default:
            return "Valor Cat No encontrado";
            }
        } else {
            return null;
        }
    }

}
