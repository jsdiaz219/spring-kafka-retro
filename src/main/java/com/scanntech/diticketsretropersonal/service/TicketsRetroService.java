package com.scanntech.diticketsretropersonal.service;

import com.scanntech.diticketsretropersonal.avro.MovementAv;
import com.scanntech.diticketsretropersonal.dto.MovementStatus;
import com.scanntech.diticketsretropersonal.dto.ReprocessDataDto;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class TicketsRetroService {
    private static final Logger log = LoggerFactory.getLogger(TicketsRetroService.class);
    private static final String dir = System.getProperty("user.dir");

    private final String baseDir;

    public TicketsRetroService(@Value("${movements.base-dir:NONE}") String baseDir) {
        this.baseDir = baseDir.equals("NONE") ? dir + "/local-data/" : baseDir;
    }

    public List<Path> reprocess(ReprocessDataDto reprocessDataDto) throws IOException {
        String pathRegex = reprocessDataDto.toRegexString(MovementStatus.PENDING);
        log.info("adentro");
        return this.findFiles(pathRegex);
    }

    private List<Path> findFiles(String pathRegex) throws IOException {
        Pattern pattern = Pattern.compile(pathRegex);
        try (Stream<Path> files = Files.walk(Paths.get(this.baseDir), FileVisitOption.FOLLOW_LINKS)) {
            return files
                    .filter(Files::isRegularFile)
                    .filter(path -> pattern.matcher(path.toString()).matches())
                    .collect(Collectors.toList());
        }
    }

    public void saveNewMovements(List<MovementAv> movements) {
        try {
            Map<String, List<MovementAv>> groupedMovements = groupMovementsByKey(movements);

            for (Map.Entry<String, List<MovementAv>> entry: groupedMovements.entrySet()) {
                log.info(entry.getValue().toString());
                writeToFile(entry.getValue(), ("%s/local-data/%s").formatted(baseDir, entry.getKey()));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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

    private void writeToFile(List<MovementAv> movements, String filePath) throws IOException {
        checkParentDirectories(filePath);
        DatumWriter<MovementAv> movementDatumWriter = new SpecificDatumWriter<>(MovementAv.class);

        File file = new File(filePath);
        DataFileWriter<MovementAv> dataFileWriter;

        if (file.exists()) {
            dataFileWriter = new DataFileWriter<>(movementDatumWriter);
            dataFileWriter.appendTo(file);
        } else {
            dataFileWriter = new DataFileWriter<>(movementDatumWriter);
            dataFileWriter.create(movements.get(0).getSchema(), file);
        }

        for (MovementAv movementAv: movements) {
            try {
                dataFileWriter.append(movementAv);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        dataFileWriter.close();
    }

    private Map<String, List<MovementAv>> groupMovementsByKey(List<MovementAv> movements) {
        return movements.stream().collect(Collectors.groupingBy(this::generateMovementKey));
    }

    private String generateMovementKey(MovementAv movementAv) {
        String[] dateSplit = movementAv.getCommercialDate().toString().split("/");
        String month = dateSplit[1];
        String year = dateSplit[2];
        String company = String.valueOf(movementAv.getCompany());
        String store = String.valueOf(movementAv.getStore());

        return "%s-%s/%s/%s/%s.avro".formatted(month, year, company, MovementStatus.PENDING, store);
    }
}
