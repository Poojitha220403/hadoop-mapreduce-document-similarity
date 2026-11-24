package com.example;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.example.controller.DocumentSimilarityDriver;

/**
 * Runs the real two-stage MapReduce pipeline (DocumentSimilarityDriver) in
 * Hadoop's local mode and checks the Jaccard similarities it produces.
 */
public class DocumentSimilarityJobTest {

    private static final Pattern LINE = Pattern.compile("^(\\S+), (\\S+) Similarity: (\\S+)\\s*$");

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    /** Runs the driver on the given input file and returns {"A|B" -> similarity}, A sorted before B. */
    private Map<String, Double> run(File input) throws Exception {
        File outBase = new File(tmp.getRoot(), "out-" + System.nanoTime());
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        int exit = ToolRunner.run(conf, new DocumentSimilarityDriver(),
                new String[] {input.getAbsolutePath(), outBase.getAbsolutePath()});
        assertEquals("driver exit code", 0, exit);

        Map<String, Double> result = new HashMap<String, Double>();
        File finalDir = new File(outBase, "final");
        File[] parts = finalDir.listFiles();
        assertTrue("no output written", parts != null);
        for (File part : parts) {
            if (!part.getName().startsWith("part-")) continue;
            for (String line : Files.readAllLines(part.toPath(), StandardCharsets.UTF_8)) {
                if (line.trim().isEmpty()) continue;
                Matcher m = LINE.matcher(line);
                assertTrue("unexpected output line: " + line, m.matches());
                String a = m.group(1), b = m.group(2);
                String key = a.compareTo(b) <= 0 ? a + "|" + b : b + "|" + a;
                result.put(key, Double.valueOf(m.group(3)));
            }
        }
        return result;
    }

    private File write(String name, String... lines) throws IOException {
        File f = new File(tmp.getRoot(), name);
        Files.write(f.toPath(), java.util.Arrays.asList(lines), StandardCharsets.UTF_8);
        return f;
    }

    @Test
    public void jaccardIsIntersectionOverUnion() throws Exception {
        // Doc1 = {apple, banana, cherry}, Doc2 = {apple, banana, date}
        // intersection 2, union 3 + 3 - 2 = 4  ->  0.50
        File in = write("a.txt", "Doc1 apple banana cherry", "Doc2 apple banana date");
        Map<String, Double> sims = run(in);
        assertEquals(1, sims.size());
        assertEquals(0.50, sims.get("Doc1|Doc2"), 0.0001);
    }

    @Test
    public void documentsSharingNoWordsProduceNoPair() throws Exception {
        File in = write("b.txt", "Doc1 apple banana", "Doc2 apple cherry", "Doc3 xylophone zebra");
        Map<String, Double> sims = run(in);
        assertTrue(sims.containsKey("Doc1|Doc2"));
        assertFalse(sims.containsKey("Doc1|Doc3"));
        assertFalse(sims.containsKey("Doc2|Doc3"));
    }

    @Test
    public void caseAndPunctuationAreNormalisedAndRepeatsCountOnce() throws Exception {
        // Doc1 normalises to {apple, banana}; Doc2 to {apple}: inter 1, union 2 + 1 - 1 = 2 -> 0.50
        File in = write("c.txt", "Doc1 Apple, BANANA! banana", "Doc2 apple apple apple");
        Map<String, Double> sims = run(in);
        assertEquals(0.50, sims.get("Doc1|Doc2"), 0.0001);
    }

    @Test
    public void identicalWordSetsScoreOne() throws Exception {
        File in = write("d.txt", "Doc1 red green blue", "Doc2 blue green red");
        assertEquals(1.0, run(in).get("Doc1|Doc2"), 0.0001);
    }

    @Test
    public void blankLinesAndDocumentsWithoutTextAreIgnored() throws Exception {
        File in = write("e.txt", "Doc1 apple banana", "", "Doc2", "Doc3 apple banana");
        Map<String, Double> sims = run(in);
        assertEquals(1, sims.size());
        assertEquals(1.0, sims.get("Doc1|Doc3"), 0.0001);
    }

    @Test
    public void everyPairAmongThreeOverlappingDocumentsIsReported() throws Exception {
        File in = write("f.txt", "Doc1 a b c", "Doc2 b c d", "Doc3 c d e");
        Map<String, Double> sims = run(in);
        assertEquals(3, sims.size());
        assertEquals(2.0 / 4.0, sims.get("Doc1|Doc2"), 0.0001);  // {b,c} / {a,b,c,d}
        assertEquals(1.0 / 5.0, sims.get("Doc1|Doc3"), 0.0001);  // {c} / {a,b,c,d,e}
        assertEquals(2.0 / 4.0, sims.get("Doc2|Doc3"), 0.0001);  // {c,d} / {b,c,d,e}
    }

    @Test
    public void matchesTheCommittedResultsForTheProvidedDataset() throws Exception {
        File dataset = new File("dataset/ds1.txt");
        File expectedFile = new File("output/results_ds1.txt");
        assertTrue("dataset missing", dataset.exists());
        assertTrue("expected results missing", expectedFile.exists());

        Map<String, Double> expected = new HashMap<String, Double>();
        List<String> lines = Files.readAllLines(expectedFile.toPath(), StandardCharsets.UTF_8);
        for (String line : lines) {
            Matcher m = LINE.matcher(line);
            if (!m.matches()) continue;
            String a = m.group(1), b = m.group(2);
            expected.put(a.compareTo(b) <= 0 ? a + "|" + b : b + "|" + a, Double.valueOf(m.group(3)));
        }
        assertFalse("could not parse the expected results", expected.isEmpty());

        Map<String, Double> actual = run(dataset);
        assertEquals(expected.keySet(), actual.keySet());
        for (Map.Entry<String, Double> e : expected.entrySet()) {
            assertEquals(e.getKey(), e.getValue(), actual.get(e.getKey()), 0.0101);
        }
    }

    @Test
    public void driverRejectsWrongArguments() throws Exception {
        assertEquals(1, new DocumentSimilarityDriver().run(new String[0]));
    }
}
