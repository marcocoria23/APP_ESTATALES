/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package plantillas;

import java.awt.Desktop;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import javax.swing.JOptionPane;

/**
 *
 * @author ANTONIO.CORIA
 */
public class AbrirPdf {
public static void abrirPdfDesdeResources(String resourcePath) {
        try (InputStream in = AbrirPdf.class.getResourceAsStream(resourcePath)) {

            if (in == null) {
                JOptionPane.showMessageDialog(null,
                        "No se encontró el PDF: " + resourcePath,
                        "Error", JOptionPane.ERROR_MESSAGE);
                return;
            }

            // Crear archivo temporal
            Path temp = Files.createTempFile("documento_", ".pdf");
            temp.toFile().deleteOnExit();

            // Copiar PDF del jar al archivo temporal
            Files.copy(in, temp, StandardCopyOption.REPLACE_EXISTING);

            // Abrir con el visor por defecto
            if (Desktop.isDesktopSupported()) {
                Desktop.getDesktop().open(temp.toFile());
            } else {
                JOptionPane.showMessageDialog(null,
                        "Desktop no soportado.",
                        "Error", JOptionPane.ERROR_MESSAGE);
            }

        } catch (Exception e) {
            JOptionPane.showMessageDialog(null,
                    "No se pudo abrir el PDF: " + e.getMessage(),
                    "Error", JOptionPane.ERROR_MESSAGE);
            e.printStackTrace();
        }
    }
}