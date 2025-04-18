package com.bolddb;

import java.math.BigInteger;
import java.nio.file.Path;
import java.util.Random;

public class TableBenchmark {
    public static void main(String[] args) throws Exception {
        int numProfiles = 2000; // Enough to fill ~1GB file
        int queries = 1000;
        Path dbDir = Path.of("benchmarkdb");
        String tableName = "customers";
        Table table = new Table(tableName, dbDir);

        // Insert realistic customer profiles
        System.out.println("Inserting customer profiles...");
        for (int i = 0; i < numProfiles; i++) {
            String customerId = "CUST" + i;
            Row row = new Row(customerId.getBytes());
            row.setAttribute("customerId", customerId);
            row.setAttribute("name", "Customer " + i);
            row.setAttribute("email", "customer" + i + "@example.com");
            row.setAttribute("address", "123 Main St, City " + (i % 1000));
            row.setAttribute("phone", String.format("%010d", 9000000000L + i));
            table.insert(row);
        }
        table.save();
        System.out.println("Insertion complete.");

        // Print actual file size
        Path tableFile = dbDir.resolve(tableName + ".table");
        long fileSize = java.nio.file.Files.size(tableFile);
        System.out.printf("Table file size: %.2f MB (%.2f GB)%n", fileSize / (1024.0 * 1024.0), fileSize / (1024.0 * 1024.0 * 1024.0));

        // Benchmark point queries and count page accesses
        Random rand = new Random(42);
        long totalTimeNs = 0;
        BigInteger totalPagesAccessed = new BigInteger("0");
        for (int i = 0; i < queries; i++) {
            String customerId = "CUST" + rand.nextInt(numProfiles);

            long start = System.nanoTime();
            Table.GetResult result = table.get(customerId.getBytes());
            long duration = System.nanoTime() - start;
            totalTimeNs += duration;
            totalPagesAccessed = totalPagesAccessed.add(BigInteger.valueOf(result.pagesAccessed));
            if (result.row == null) throw new RuntimeException("Customer not found: " + customerId);
        }
        System.out.printf("Average point query time: %.2f us\n", totalTimeNs / (queries * 1000.0));
        System.out.printf("Average pages accessed per query: %s\n", totalPagesAccessed.divide(BigInteger.valueOf(queries)).toString());
    }
}
