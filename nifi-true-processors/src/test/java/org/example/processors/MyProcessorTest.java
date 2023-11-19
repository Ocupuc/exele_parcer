package org.example.processors;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
public class MyProcessorTest {

    private TestRunner testRunner;

    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(MyProcessor.class);
        testRunner.setProperty(MyProcessor.MY_PROPERTY, "Test-XLS");
    }

    @Test
    public void testProcessor() {

    }
//    @Test
//    public void testOnTrigger() throws IOException {
//        // Путь к тестовому файлу Excel в директории ресурсов
//        Path testFilePath = Paths.get("src/test/resources/Unsupported.xls");
//        byte[] fileContent = Files.readAllBytes(testFilePath);
//
//
//
//        // Запуск процессора с тестовым FlowFile
//        testRunner.enqueue(fileContent);
//        testRunner.run();
//
//        // Проверка, что процессор корректно обработал FlowFile
//        testRunner.assertTransferCount(MyProcessor.REL_OK, 1);
//        // Дополнительные проверки...
//    }
//
//    @Test
//    public  void testForFailSheet() throws IOException {
//        // Путь к тестовому файлу Excel в директории ресурсов
//        Path testFilePath = Paths.get("src/test/resources/Unsupported.xls");
//        byte[] fileContent = Files.readAllBytes(testFilePath);
//        testRunner.setProperty(MyProcessor.MY_PROPERTY, "неверное значение");
//
//
//        testRunner.enqueue(fileContent);
//        testRunner.run();
//
//        testRunner.assertTransferCount(MyProcessor.REL_FAIL, 1);
//    }
//
    @Test
    public void setTestRunner() throws IOException {
        // Путь к тестовому файлу Excel в директории ресурсов
        Path testFilePath = Paths.get("src/test/resources/Спар-СМ отчет октябрь 2023.xlsx");
        byte[] fileContent = Files.readAllBytes(testFilePath);
        testRunner.setProperty(MyProcessor.MY_PROPERTY, "RRN");



        // Запуск процессора с тестовым FlowFile
        testRunner.enqueue(fileContent);
        testRunner.run();

        // Проверка, что процессор корректно обработал FlowFile
        testRunner.assertTransferCount(MyProcessor.REL_OK, 1);
        // Дополнительные проверки...
    }

}
