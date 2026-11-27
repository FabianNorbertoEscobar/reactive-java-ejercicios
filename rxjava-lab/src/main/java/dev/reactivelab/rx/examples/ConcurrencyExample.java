package dev.reactivelab.rx.examples;

import io.reactivex.rxjava3.core.*;
import io.reactivex.rxjava3.schedulers.Schedulers;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * 24: Asincronismo y concurrencia con schedulers: subscribeOn, observeOn,
 *     ejecución paralela y errores en operaciones asíncronas.
 *
 *   fuente --subscribeOn(io)--> map --observeOn(computation)--> map --> consumidor
 *   (subscribeOn fija dónde empieza la fuente; observeOn cambia el hilo de lo que sigue)
 *
 * 1. Se muestran los hilos que ejecutan cada etapa.
 * 2. Dos tareas independientes de 300 ms se ejecutan en paralelo con zip + subscribeOn:
 *    el total ronda 300 ms y no 600 ms.
 * 3. Un fallo dentro de una tarea asíncrona se recupera con onErrorReturnItem o se propaga.
 * 4. Compara con {@code ConcurrencyExample} del módulo Mutiny.
 *
 * Resultado esperado (los nombres de hilo varían):
 *   fuente: RxCachedThreadScheduler-1
 *   tras observeOn: RxComputationThreadPool-1
 *   paralelo: [A, B] en ~300 ms
 *   flatMap concurrente: [2, 4, 6, 8]
 *   error recuperado: valor alternativo
 *   error propagado: fallo en B
 */
public final class ConcurrencyExample {
    private ConcurrencyExample() {}

    /** Resultado del ejemplo de ejecución paralela. */
    public record Parallel(String result, long millis) {}

    public static String sourceThread() {
        return Observable.fromCallable(() -> Thread.currentThread().getName())
                .subscribeOn(Schedulers.io()).blockingFirst();
    }

    public static String threadAfterObserveOn() {
        return Observable.just(1)
                .subscribeOn(Schedulers.io())
                .observeOn(Schedulers.computation())
                .map(n -> Thread.currentThread().getName()).blockingFirst();
    }

    private static Single<String> task(String name, long millis) {
        return task(name, millis, null, null);
    }

    private static Single<String> task(
            String name, long millis, CountDownLatch started, CountDownLatch release) {
        return Single.<String>create(emitter -> {
            try {
                if (started != null) {
                    started.countDown();
                    release.await();
                }
                Thread.sleep(millis);
                emitter.onSuccess(name);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                emitter.tryOnError(e);
            }
        }).subscribeOn(Schedulers.io());
    }

    public static Parallel parallelTasks() {
        return parallelTasks(null, null);
    }

    static Parallel parallelTasks(CountDownLatch started, CountDownLatch release) {
        long start = System.nanoTime();
        var result = Single.zip(
                task("A", 300, started, release),
                task("B", 300, started, release),
                (a, b) -> "[" + a + ", " + b + "]").blockingGet();
        return new Parallel(result, (System.nanoTime() - start) / 1_000_000);
    }

    /** flatMap lanza las tareas internas a la vez; maxConcurrency limita cuántas. */
    public static List<Integer> concurrentFlatMap() {
        return Flowable.range(1, 4)
                .flatMap(n -> Flowable.fromCallable(() -> n * 2).subscribeOn(Schedulers.io()), 2)
                .sorted().toList().blockingGet();
    }

    public static String recoveredError() {
        return Single.<String>fromCallable(() -> {
            throw new IllegalStateException("fallo asíncrono");
        }).subscribeOn(Schedulers.io()).onErrorReturnItem("valor alternativo").blockingGet();
    }

    public static String propagatedError() {
        var failing = Single.<String>fromCallable(() -> {
            throw new IllegalStateException("fallo en B");
        }).subscribeOn(Schedulers.io());
        try {
            return Single.zip(task("A", 10), failing, (a, b) -> a + b).blockingGet();
        } catch (RuntimeException e) {
            // blockingGet relanza la excepción original (o la envuelve si es comprobada).
            return e.getMessage();
        }
    }

    public static List<String> run() {
        var log = new ArrayList<String>();
        log.add("fuente: " + sourceThread());
        log.add("tras observeOn: " + threadAfterObserveOn());
        var parallel = parallelTasks();
        log.add("paralelo: " + parallel.result() + " en ~" + parallel.millis() + " ms");
        log.add("flatMap concurrente: " + concurrentFlatMap());
        log.add("error recuperado: " + recoveredError());
        log.add("error propagado: " + propagatedError());
        return log;
    }

    public static void main(String[] args) {
        run().forEach(System.out::println);
    }
}
