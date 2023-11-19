/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.example.processors;

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.poi.ss.usermodel.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
public class MyProcessor extends AbstractProcessor {

    LocalDate current = LocalDate.now();
    LocalDate oneMonthAgo = current.minusMonths(1);
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MMMM yyyy", new Locale("ru"));
    String formattedDate = oneMonthAgo.format(formatter);

    public static final PropertyDescriptor MY_PROPERTY = new PropertyDescriptor
            .Builder().name("MY_PROPERTY")
            .displayName("My property")
            .description("Example Property")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_OK = new Relationship.Builder()
            .name("REL_OK")
            .description("Example relationship ok")
            .build();

    public static final Relationship REL_FAIL = new Relationship.Builder()
            .name("REL_FAIL")
            .description("Example relationship fail")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
        descriptors.add(MY_PROPERTY);
        descriptors = Collections.unmodifiableList(descriptors);

        relationships = new HashSet<>();
        relationships.add(REL_OK);
        relationships.add(REL_FAIL);
        relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

//    @Override
//    public void onTrigger(final ProcessContext context, final ProcessSession session) {
//        FlowFile flowFile = session.get();
//        if (flowFile == null) {
//            return;
//        }
//        final String sheetName = context.getProperty(MY_PROPERTY).getValue();
//
//        try (Workbook workbook = WorkbookFactory.create(session.read(flowFile))) {
//
//            Sheet sheet = workbook.getSheet(sheetName);
//            if (sheet == null) {
//                throw new IllegalAccessException("Sheet with name '" + sheetName + "' does not exist in the workbook");
//            }
//            DataFormatter formatter = new DataFormatter();
//            StringBuilder result = new StringBuilder();
//
//            Map<String, String> replacements = getStringStringMap();
//
//            Set<String> exclusions = new HashSet<>();
//            exclusions.add("\"matching_amount\"");
//            exclusions.add("\"return_amount\"");
//
//            Set<Integer> columnsToSkip = new HashSet<>(); // Сет для хранения индексов колонок, которые нужно пропустить
//
//            for (Row row : sheet) {
//
//                for (Cell cell : row) {
//                    String text = "\"" + formatter.formatCellValue(cell) + "\"";
//                    int columnIndex = cell.getColumnIndex();
//
//                    if (row.getRowNum() == 0) { // Проверка для заголовка
//                        if (exclusions.contains(text)) {
//                            columnsToSkip.add(columnIndex); // Добавляем индекс колонки в сет для пропуска
//                            continue; // Пропускаем текущую ячейку
//                        }
//                        if (replacements.containsKey(text)) {
//                            result.append(replacements.get(text));
//                        } else {
//                            throw new IllegalAccessException("The header is set incorrectly: " + text);
//                        }
//                    } else {
//                        if (!columnsToSkip.contains(columnIndex)) { // Для всех строк кроме заголовка проверяем, не находится ли индекс колонки в сете пропуска
//                            result.append(text); // Добавляем текст ячейки в StringBuilder
//                        }
//                    }
//
//                    if (!columnsToSkip.contains(columnIndex)) { // Добавляем запятую, если колонка не пропущена
//                        result.append(",");
//                    }
//                }
//
////                if (result.length() > 0 && result.charAt(result.length() - 1) == ',') {
////                    result.setLength(result.length() - 1); // Удаляем последнюю запятую в строке
////                }
//                if (row.getRowNum() == 0) {
//                    result.append("\"period\"");
//                } else result.append("\"" + formattedDate + "\"");
//                result.append("\n");
//            }
//
//
//                    System.out.println(result);
//            flowFile = session.write(flowFile, outputStream -> {
//                outputStream.write(result.toString().getBytes(StandardCharsets.UTF_8));
//
//            });
//            session.transfer(flowFile, REL_OK);
//
//        } catch (Exception e) {
//            e.printStackTrace();
//
//
//            session.transfer(flowFile, REL_FAIL);
//        }
//    }

    private Sheet initializeSheet(final ProcessSession session, FlowFile flowFile, final String sheetName) throws IOException, IllegalAccessException {
        Workbook workbook = WorkbookFactory.create(session.read(flowFile));
        Sheet sheet = workbook.getSheet(sheetName);

        if (sheet == null) {
            throw new IllegalAccessException("Sheet with name '" + sheetName + "' does not exist in the workbook");
        }

        return sheet;
    }

    private Set<Integer> processHeader(Row headerRow, Map<String, String> replacements, Set<String> exclusions) throws IllegalAccessException {
        Set<Integer> columnsToSkip = new HashSet<>();
        DataFormatter formatter = new DataFormatter();

        for (Cell cell : headerRow) {
            String text = "\"" + formatter.formatCellValue(cell) + "\"";
            int columnIndex = cell.getColumnIndex();

            if (exclusions.contains(text)) {
                columnsToSkip.add(columnIndex);
                continue;
            }
            if (!replacements.containsKey(text)) {
                throw new IllegalAccessException("The header is set incorrectly: " + text);
            }
        }

        return columnsToSkip;
    }

    private String processDataRows(Sheet sheet, Set<Integer> columnsToSkip, Map<String, String> replacements) {
        StringBuilder result = new StringBuilder();
        DataFormatter formatter = new DataFormatter();
        boolean isHeader = true;

        for (Row row : sheet) {
            for (Cell cell : row) {
                int columnIndex = cell.getColumnIndex();
                if (isHeader || !columnsToSkip.contains(columnIndex)) {
                    String text = "\"" + formatter.formatCellValue(cell) + "\"";
                    if (isHeader) {
                        text = replacements.getOrDefault(text, text);
                    }
                    result.append(text).append(",");
                }
            }
            if (isHeader) {
                result.append("\"period\"");
                isHeader = false;
            } else {
                result.append("\"" + formattedDate + "\"");
            }
            result.append("\n");
        }

        return result.toString();
    }
    private void writeData(final ProcessSession session, FlowFile flowFile, String data) {
        flowFile = session.write(flowFile, outputStream -> {
            outputStream.write(data.getBytes(StandardCharsets.UTF_8));
        });
        session.transfer(flowFile, REL_OK);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final String sheetName = context.getProperty(MY_PROPERTY).getValue();

        try {
            Sheet sheet = initializeSheet(session, flowFile, sheetName);
            Row headerRow = sheet.getRow(0);
            Map<String, String> replacements = getStringStringMap();
            Set<String> exclusions = new HashSet<>();
            exclusions.add("\"matching_amount\"");
            exclusions.add("\"return_amount\"");

            Set<Integer> columnsToSkip = processHeader(headerRow, replacements, exclusions);
            String data = processDataRows(sheet, columnsToSkip, replacements);
            writeData(session, flowFile, data);

        } catch (Exception e) {
            e.printStackTrace();
            session.transfer(flowFile, REL_FAIL);
        }
    }


    private static Map<String, String> getStringStringMap() {
        Map<String, String> replacements = new HashMap<>();
        replacements.put("\"ID Ритейлера\"", "\"id_retail\"");
        replacements.put("\"Ритейлер\"", "Retailer\"");
        replacements.put("\"ИНН\"", "\"inn\"");
        replacements.put("\"Юр.лицо\"", "\"name\"");
        replacements.put("\"Магазин\"", "\"address\"");
        replacements.put("\"Банковский счёт\"", "\"account\"");
        replacements.put("\"Тип операции\"", "\"oper\"");
        replacements.put("\"Категория\"", "\"cat\"");
        replacements.put("\"Дата авторизации\"", "\"date_auth\"");
        replacements.put("\"Дата транзакции\"", "\"date_tr\"");
        replacements.put("\"ИД Авторизации\"", "\"id_auth\"");
        replacements.put("\"ИД Транзакции\"", "\"id_tr\"");
        replacements.put("\"RRN\"", "\"rrn\"");
        replacements.put("\"Сумма\"", "\"summa\"");
        replacements.put("\"Номер карты\"", "\"bankcard\"");
        replacements.put("\"Мерчант ID\"", "\"merchid\"");
        replacements.put("\"Мерчант\"", "\"merch\"");
        replacements.put("\"Номер заказа\"", "\"id_order\"");
        replacements.put("\"fiscal_checksum\"", "\"fiscal_checksum\"");
        replacements.put("\"fiscal_document_number\"", "\"fiscal_document_number\"");
        replacements.put("\"fiscal_secret\"", "\"fiscal_secret\"");
        return replacements;
    }
}
