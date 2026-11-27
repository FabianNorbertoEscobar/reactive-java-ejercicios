package dev.reactivelab.mutiny.examples;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 24: Asincronismo y concurrencia con executors: runSubscriptionOn, emitOn,
 *     ejecución paralela y errores en operaciones asíncronas.
 *
 *   fuente --runSubscriptionOn(pool)--> map --emitOn(pool)--> map --> consumidor
 *   (Mutiny no tiene schedulers: usa Executor de Java; runSubscriptionOn ≈ subscribeOn,
 *    emitOn ≈ observeOn)
 *
 * 1. Se muestran los hilos que ejecutan cada etapa.
 * 2. Dos tareas independientes de 300 ms se ejecutan en paralelo con combine().all():
 *    el total ronda 300 ms y no 600 ms.
 * 3. Un fallo dentro de una tarea asíncrona se recupera con recoverWithItem o se propaga.
 * 4. Compara con {@code ConcurrencyExample} del módulo RxJava.
 *
 * Resultado esperado (los nombres de hilo varían):
 *   fuente: worker-1
 *   tras emitOn: emit-1
 *   paralelo: [A, B] en ~300 ms
 *   flatMap concurrente: [2, 4, 6, 8]
 *   error recuperado: valor alternativo
 *   error propagado: fallo en B
 */
public final class ConcurrencyExample {
    private ConcurrencyExample() {}

    /** Resultado del ejemplo de ejecución paralela. */
    public record Parallel(String result, long millis) {}

    private static ExecutorService pool(String prefix) {
        var counter = new AtomicInteger(1);
        return Executors.newFixedThreadPool(4, runnable -> {
            var thread = new Thread(runnable, prefix + "-" + counter.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        });
    }

    public static String sourceThread() {
        try (var workers = pool("worker")) {
            return Uni.createFrom().item(() -> Thread.currentThread().getName())
                    .runSubscriptionOn(workers).await().indefinitely();
        }
    }

    public static String threadAfterEmitOn() {
        try (var workers = pool("worker"); var emitters = pool("emit")) {
            return Uni.createFrom().item(1)
                    .runSubscriptionOn(workers)
                    .emitOn(emitters)
                    .map(n -> Thread.currentThread().getName()).await().indefinitely();
        }
    }

    private static Uni<String> task(String name, long millis, Executor executor) {
        return Uni.createFrom().item(() -> {
            sleep(millis);
            return name;
        }).runSubscriptionOn(executor);
    }

    public static Parallel parallelTasks() {
        try (var workers = pool("worker")) {
            long start = System.nanoTime();
            var result = Uni.combine().all().unis(task("A", 300, workers), task("B", 300, workers))
                    .with((a, b) -> "[" + a + ", " + b + "]").await().indefinitely();
            return new Parallel(result, (System.nanoTime() - start) / 1_000_000);
        }
    }

    /** transformToUniAndMerge lanza las tareas internas a la vez; el argumento limita cuántas. */
    public static List<Integer> concurrentFlatMap() {
        try (var workers = pool("worker")) {
            return Multi.createFrom().range(1, 5).onItem()
                    .transformToUni(n -> Uni.createFrom().item(() -> n * 2).runSubscriptionOn(workers))
                    .merge(2)
                    .collect().asList().await().indefinitely().stream().sorted().toList();
        }
    }

    public static String recoveredError() {
        try (var workers = pool("worker")) {
            return Uni.createFrom().<String>item(() -> {
                throw new IllegalStateException("fallo asíncrono");
            }).runSubscriptionOn(workers).onFailure().recoverWithItem("valor alternativo")
                    .await().indefinitely();
        }
    }

    public static String propagatedError() {
        try (var workers = pool("worker")) {
            var failing = Uni.createFrom().<String>item(() -> {
                throw new IllegalStateException("fallo en B");
            }).runSubscriptionOn(workers);
            return Uni.combine().all().unis(task("A", 10, workers), failing)
                    .with((a, b) -> a + b).await().indefinitely();
        } catch (RuntimeException e) {
            // await() relanza el fallo original si es una RuntimeException.
            return e.getMessage();
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    public static List<String> run() {
        var log = new ArrayList<String>();
        log.add("fuente: " + sourceThread());
        log.add("tras emitOn: " + threadAfterEmitOn());
        var parallel = parallelTasks();
        log.add("paralelo: " + parallel.result() + " en ~" + parallel.millis() + " ms");
        log.add("flatMap concurrente: " + concurrentFlatMap());
        log.add("error recuperado: " + recoveredError());
        log.add("error propagado: " + propagatedError());
        return log;
    }

    public static void main(String[] args) {
        ExampleRuntime.print(run());
    }
}
