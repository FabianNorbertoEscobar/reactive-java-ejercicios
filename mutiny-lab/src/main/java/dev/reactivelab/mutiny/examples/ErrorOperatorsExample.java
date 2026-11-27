package dev.reactivelab.mutiny.examples;

import dev.reactivelab.domain.FakeRepository;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 28: Manejo de errores: subscribe con fallo, recoverWithItem, recoverWithMulti, retry,
 *     retry().when() y cadenas.
 *
 *   fuente --X (fallo)--> onFailure().[recoverWithItem | recoverWithMulti | retry] --> consumidor
 *
 * Un fallo es una señal TERMINAL: tras él no se emiten más elementos. onFailure() agrupa los
 * operadores que lo sustituyen por un valor, otra fuente o una nueva suscripción.
 *
 * 1. subscribe().with(onItem, onFailure) recibe el fallo sin recuperarse.
 * 2. recoverWithItem / recoverWithMulti ≈ onErrorReturn / onErrorResumeNext.
 * 3. retry().atMost(n) ≈ retry(n); retry().when(...) ≈ retryWhen; withBackOff añade espera.
 * 4. En una cadena, un fallo en cualquier etapa salta las etapas siguientes hasta el manejador.
 * 5. Compara con {@code ErrorOperatorsExample} del módulo RxJava.
 *
 * Resultado esperado:
 *   onError: items=[10, 5] error=/ by zero
 *   onErrorReturn: [10, 5, -1]
 *   onErrorResumeNext: [10, 5, 99]
 *   retry(2): resultado=OK intentos=3
 *   retryWhen con espera: resultado=OK intentos=3
 *   retry con backoff: resultado=OK intentos=3
 *   cadena: DESCONOCIDO
 */
public final class ErrorOperatorsExample {
    private ErrorOperatorsExample() {}

    /** 10/1, 10/2 y luego 10/0 lanza ArithmeticException. */
    private static Multi<Integer> divisions() {
        return Multi.createFrom().items(1, 2, 0, 4).onItem().transform(n -> 10 / n);
    }

    public static String onError() {
        var items = new ArrayList<Integer>();
        var error = new ArrayList<String>();
        divisions().subscribe().with(items::add, e -> error.add(e.getMessage()));
        return "items=" + items + " error=" + error.get(0);
    }

    public static List<Integer> onErrorReturn() {
        return divisions().onFailure().recoverWithItem(-1).collect().asList().await().indefinitely();
    }

    public static List<Integer> onErrorResumeNext() {
        return divisions().onFailure().recoverWithMulti(error -> Multi.createFrom().item(99))
                .collect().asList().await().indefinitely();
    }

    private static Uni<String> flaky(AtomicInteger attempts, int failures) {
        // Uni.createFrom().item(Supplier) se evalúa en cada suscripción, incluidos los reintentos.
        return Uni.createFrom().item(() -> {
            if (attempts.incrementAndGet() <= failures) throw new IllegalStateException("fallo temporal");
            return "OK";
        });
    }

    public static String retry() {
        var attempts = new AtomicInteger();
        var result = flaky(attempts, 2).onFailure().retry().atMost(2).await().indefinitely();
        return "resultado=" + result + " intentos=" + attempts.get();
    }

    /** Reintenta tras 10 ms cada vez que falla; el flujo de fallos decide cuándo reintentar. */
    public static String retryWhen() {
        var attempts = new AtomicInteger();
        var result = flaky(attempts, 2).onFailure().retry()
                .when(errors -> errors.onItem().transformToUniAndConcatenate(
                        error -> Uni.createFrom().item(error).onItem().delayIt().by(Duration.ofMillis(10))))
                .await().indefinitely();
        return "resultado=" + result + " intentos=" + attempts.get();
    }

    /** withBackOff duplica la espera entre intentos: la forma corta de un retryWhen exponencial. */
    public static String retryWithBackoff() {
        var attempts = new AtomicInteger();
        var result = flaky(attempts, 2).onFailure().retry()
                .withBackOff(Duration.ofMillis(10), Duration.ofMillis(100)).atMost(3)
                .await().indefinitely();
        return "resultado=" + result + " intentos=" + attempts.get();
    }

    /** Un fallo en la primera etapa se salta el resto y llega al manejador final. */
    public static String chain() {
        var repo = new FakeRepository();
        return Uni.createFrom().item(() -> repo.findUser(999).orElseThrow(() -> new NoSuchElementException("sin usuario")))
                .map(user -> user.name().toUpperCase())
                .chain(name -> Uni.createFrom().item(() -> name + ": " + repo.fetchStatus(100)))
                .onFailure().invoke(e -> { /* aquí se registraría el fallo sin consumirlo */ })
                .onFailure().recoverWithItem("DESCONOCIDO")
                .await().indefinitely();
    }

    public static List<String> run() {
        var log = new ArrayList<String>();
        log.add("onError: " + onError());
        log.add("onErrorReturn: " + onErrorReturn());
        log.add("onErrorResumeNext: " + onErrorResumeNext());
        log.add("retry(2): " + retry());
        log.add("retryWhen con espera: " + retryWhen());
        log.add("retry con backoff: " + retryWithBackoff());
        log.add("cadena: " + chain());
        return log;
    }

    public static void main(String[] args) {
        ExampleRuntime.print(run());
    }
}
