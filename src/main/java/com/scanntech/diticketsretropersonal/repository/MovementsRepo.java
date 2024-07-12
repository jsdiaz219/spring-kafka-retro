package com.scanntech.diticketsretropersonal.repository;

import com.scanntech.di.commons.kafka.avro.model.TransaccionPendienteAv;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Repository
public class MovementsRepo implements IMovementsRepo{
    private static final String dir = System.getProperty("user.dir");
    private final String baseDir;

    public MovementsRepo(@Value("${movements.base-dir:NONE}") String baseDir) {
        this.baseDir = baseDir.equals("NONE") ? dir + "/local-data/" : baseDir;
    }

    @Override
    public void saveManyMovementsToFile(String path, List<TransaccionPendienteAv> movements) throws IOException {
        String absolutePath = this.baseDir + path;
        this.checkParentDirectories(absolutePath);
        DatumWriter<TransaccionPendienteAv> movementDatumWriter = new SpecificDatumWriter<>(TransaccionPendienteAv.class);

        File file = new File(absolutePath);
        DataFileWriter<TransaccionPendienteAv> dataFileWriter;

        if (file.exists()) {
            dataFileWriter = new DataFileWriter<>(movementDatumWriter);
            dataFileWriter.appendTo(file);
        } else {
            dataFileWriter = new DataFileWriter<>(movementDatumWriter);
            dataFileWriter.create(movements.get(0).getSchema(), file);
        }

        for (TransaccionPendienteAv movementAv: movements) {
            try {
                dataFileWriter.append(movementAv);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        dataFileWriter.close();
    }

    private void checkParentDirectories(String filePath) {
        Path path = Paths.get(filePath);
        Path parent = path.getParent();

        if (parent != null) {
            try {
                Files.createDirectories(parent);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }


    @Override
    public List<TransaccionPendienteAv> findMovements(String pathRegex) throws IOException {
//        log.info("Looking for files with regex: %s".formatted(pathRegex));
        List<Path> foundFiles = findFiles(this.baseDir, pathRegex);
//        log.info("Found %d files".formatted(foundFiles.size()));

        List<TransaccionPendienteAv> movements = new ArrayList<>();
        for (Path path: foundFiles) {
            File file = path.toFile();
            movements.addAll(readMovementsFromFile(file));
        }

        return movements;
    }

    // mes-anio/empresa/{pendiente/error/finalizado}/local.avro
    private List<TransaccionPendienteAv> readMovementsFromFile(File file) throws IOException {
        List<TransaccionPendienteAv> movements = new ArrayList<>();
        DatumReader<TransaccionPendienteAv> userDatumReader = new SpecificDatumReader<>(TransaccionPendienteAv.class);
        try (DataFileReader<TransaccionPendienteAv> dataFileReader = new DataFileReader<>(file, userDatumReader)) {
            TransaccionPendienteAv movement = null;
            while (dataFileReader.hasNext()) {
                movement = dataFileReader.next(movement);
                movements.add(movement);
            }
        }
        file.delete();
        return movements;
    }

    private List<Path> findFiles(String baseDir, String pattern) throws IOException {
        List<Path> result = new ArrayList<>();
        Path basePath = Paths.get(baseDir);
        PathMatcher matcher = createPathMatcher(pattern);

        Files.walkFileTree(basePath, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                Path relativePath = basePath.relativize(file);
                if (matcher.matches(relativePath)) {
                    result.add(file);
                }
                return FileVisitResult.CONTINUE;
            }
        });

        return result;
    }

    private PathMatcher createPathMatcher(String pattern) {
        // Reemplaza los '*' con el patrón adecuado de regex
        String regexPattern = pattern
                .replace("**", ".*")
                .replace("*", "[^/]*");

        // Agrega los delimitadores de inicio y fin de la cadena
        regexPattern = "^" + regexPattern + "$";

        Pattern compiledPattern = Pattern.compile(regexPattern);

        return path -> {
            Matcher matcher = compiledPattern.matcher(path.toString());
            return matcher.matches();
        };
    }
}
