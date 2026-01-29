/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package Prepara_Info;

import Pantallas_laborales.procesando;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import javax.swing.JOptionPane;

/**
 *
 * @author ANTONIO.CORIA
 */
public class ExtraeExe {
 
    
    public static void ejecutarExeDesdeResources(String resourcePath,procesando pro,String NombreArchivo) {
    try (InputStream in = ExtraeExe.class.getResourceAsStream(resourcePath)) {
        if (in == null) {
            JOptionPane.showMessageDialog(null,
                "No se encontró el recurso: " + resourcePath,
                "Error", JOptionPane.ERROR_MESSAGE);
            return;
        }
        Path temp = Files.createTempFile(NombreArchivo+"_", ".exe");
        Files.copy(in, temp, StandardCopyOption.REPLACE_EXISTING);
        temp.toFile().deleteOnExit();
        new ProcessBuilder(
            "cmd", "/c", "start", "\"\"", "\"" + temp.toString() + "\""
        ).start();    
    } catch (Exception e) {
        e.printStackTrace();
        JOptionPane.showMessageDialog(null,
            "No se pudo ejecutar el exe\n" + e.toString(),
            "Error", JOptionPane.ERROR_MESSAGE);
    }
}
}
