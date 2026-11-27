package dev.reactivelab.mutiny.examples;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Comprueba que los ejemplos por concepto producen la salida documentada en su Javadoc. */
class ConceptExamplesTest {

    @Test
    void creationOperators() {
        assertEquals(7, CreationOperatorsExample.run().size());
        assertTrue(CreationOperatorsExample.run().contains("range: [1, 2, 3, 4, 5]"));
        assertTrue(CreationOperatorsExample.run().contains("empty: []"));
    }

    @Test
    void transformationOperatorsDifferInOrdering() {
        var log = TransformationOperatorsExample.run();
        assertTrue(log.contains("flatMap con retardos: [2, 1]"));
        assertTrue(log.contains("concatMap con retardos: [1, 2]"));
        assertTrue(log.contains("scan: [1, 3, 6, 10]"));
        assertTrue(log.contains("buffer: [[1, 2], [3, 4], [5]]"));
    }

    @Test
    void filteringOperators() {
        var log = FilteringOperatorsExample.run();
        assertTrue(log.contains("filter: [2, 2, 4]"));
        assertTrue(log.contains("skip: [2, 2, 3, 4, 5]"));
        assertTrue(log.contains("distinct: [1, 2, 3, 4, 5]"));
        assertTrue(log.contains("takeWhile: [1, 2, 2]"));
    }

    @Test
    void combiningOperators() {
        var log = CombiningOperatorsExample.run();
        assertTrue(log.contains("merge: [B1, A1]"));
        assertTrue(log.contains("concat: [A1, B1]"));
        assertTrue(log.contains("zip: [1a, 2b]"));
        assertTrue(log.contains("combineLatest: [1x, 2x, 2y]"));
    }

    @Test
    void parallelTasksOverlapInTime() throws Exception {
        var started = new CountDownLatch(2);
        var release = new CountDownLatch(1);
        var parallelResult = CompletableFuture.supplyAsync(() -> ConcurrencyExample.parallelTasks(started, release));
        boolean bothStarted;
        try {
            bothStarted = started.await(10, TimeUnit.SECONDS);
        } finally {
            release.countDown();
        }
        assertTrue(bothStarted, "ambas tareas deberían empezar antes de liberarlas");
        var parallel = parallelResult.get(10, TimeUnit.SECONDS);
        assertEquals("[A, B]", parallel.result());
        assertEquals(List.of(2, 4, 6, 8), ConcurrencyExample.concurrentFlatMap());
        assertEquals("valor alternativo", ConcurrencyExample.recoveredError());
        assertEquals("fallo en B", ConcurrencyExample.propagatedError());
    }

    @Test
    void subjectTypes() {
        assertEquals(List.of(
                "Publish  -> A=[1, 2] B=[2]",
                "Behavior -> A=[0, 1, 2] B=[1, 2]",
                "Replay   -> A=[1, 2] B=[1, 2]",
                "Replay(1)-> A=[1, 2] B=[1, 2]",
                "Async    -> A=[2] B=[2]"), SubjectTypesExample.run());
    }

    @Test
    void manualEmissionIsColdPerSubscriber() {
        assertEquals("ejecuciones=2 A=[1, 2] B=[1, 2]", ManualEmissionExample.coldEmitter());
        assertEquals("[1] liberado=true", ManualEmissionExample.cancellation());
        assertEquals(List.of(1, 2, 3, 4), ManualEmissionExample.generator());
    }

    @Test
    void backpressureStrategies() {
        assertEquals(List.of(
                "sin estrategia: fallo BackPressureFailure=true, items=[]",
                "buffer: [1, 2, 3, 4, 5]",
                "drop: [1, 2]",
                "latest: [1, 5]",
                "fuente fría con request(n): [1, 2, 3] luego [1, 2, 3, 4, 5]"), BackpressureStrategiesExample.run());
    }

    @Test
    void errorOperators() {
        assertEquals("items=[10, 5] error=/ by zero", ErrorOperatorsExample.onError());
        assertEquals(List.of(10, 5, -1), ErrorOperatorsExample.onErrorReturn());
        assertEquals(List.of(10, 5, 99), ErrorOperatorsExample.onErrorResumeNext());
        assertEquals("resultado=OK intentos=3", ErrorOperatorsExample.retry());
        assertEquals("resultado=OK intentos=3", ErrorOperatorsExample.retryWhen());
        assertEquals("DESCONOCIDO", ErrorOperatorsExample.chain());
    }
}
