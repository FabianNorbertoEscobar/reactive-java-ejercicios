package dev.reactivelab.rx.examples;

import dev.reactivelab.domain.FakeRepository;
import io.reactivex.rxjava3.core.*;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 28: Manejo de errores: onError, onErrorReturn, onErrorResumeNext, retry, retryWhen y cadenas.
 *
 *   fuente --X (error)--> [onErrorReturn | onErrorResumeNext | retry | retryWhen] --> consumidor
 *
 * Un error es una señal TERMINAL: tras onError no se emiten más elementos. Los operadores
 * de error sustituyen esa señal por un valor, otra fuente o una nueva suscripción.
 *
 * 1. subscribe(onNext, onError) recibe el fallo sin recuperarse.
 * 2. onErrorReturn / onErrorResumeNext proporcionan un valor o una fuente alternativa.
 * 3. retry reintenta de inmediato; retryWhen decide cuándo y cuántas veces (backoff).
 * 4. En una cadena, un fallo en cualquier etapa salta las etapas siguientes hasta el manejador.
 * 5. Compara con {@code ErrorOperatorsExample} del módulo Mutiny.
 *
 * Resultado esperado:
 *   onError: items=[10, 5] error=/ by zero
 *   onErrorReturn: [10, 5, -1]
 *   onErrorResumeNext: [10, 5, 99]
 *   retry(2): resultado=OK intentos=3
 *   retryWhen con espera: resultado=OK intentos=3
 *   cadena: DESCONOCIDO
 */
public final class ErrorOperatorsExample {
    private ErrorOperatorsExample() {}

    /** 10/1, 10/2 y luego 10/0 lanza ArithmeticException. */
    private static Observable<Integer> divisions() {
        return Observable.just(1, 2, 0, 4).map(n -> 10 / n);
    }

    public static String onError() {
        var items = new ArrayList<Integer>();
        var error = new ArrayList<String>();
        divisions().subscribe(items::add, e -> error.add(e.getMessage()));
        return "items=" + items + " error=" + error.get(0);
    }

    public static List<Integer> onErrorReturn() {
        return divisions().onErrorReturnItem(-1).toList().blockingGet();
    }

    public static List<Integer> onErrorResumeNext() {
        return divisions().onErrorResumeNext(error -> Observable.just(99)).toList().blockingGet();
    }

    private static Single<String> flaky(AtomicInteger attempts, int failures) {
        return Single.fromCallable(() -> {
            if (attempts.incrementAndGet() <= failures) throw new IllegalStateException("fallo temporal");
            return "OK";
        });
    }

    public static String retry() {
        var attempts = new AtomicInteger();
        var result = flaky(attempts, 2).retry(2).blockingGet();
        return "resultado=" + result + " intentos=" + attempts.get();
    }

    /** Hasta 3 reintentos con espera creciente: 10, 20 y 30 ms. */
    public static String retryWhen() {
        var attempts = new AtomicInteger();
        var result = flaky(attempts, 2)
                .retryWhen(errors -> errors.zipWith(Flowable.range(1, 3), (error, attempt) -> attempt)
                        .flatMap(attempt -> Flowable.timer(10L * attempt, TimeUnit.MILLISECONDS)))
                .blockingGet();
        return "resultado=" + result + " intentos=" + attempts.get();
    }

    /** Un fallo en la primera etapa se salta el resto y llega al manejador final. */
    public static String chain() {
        var repo = new FakeRepository();
        return Single.fromCallable(() -> repo.findUser(999).orElseThrow(() -> new NoSuchElementException("sin usuario")))
                .map(user -> user.name().toUpperCase())
                .flatMap(name -> Single.fromCallable(() -> name + ": " + repo.fetchStatus(100)))
                .doOnError(e -> { /* aquí se registraría el fallo sin consumirlo */ })
                .onErrorReturnItem("DESCONOCIDO")
                .blockingGet();
    }

    public static List<String> run() {
        var log = new ArrayList<String>();
        log.add("onError: " + onError());
        log.add("onErrorReturn: " + onErrorReturn());
        log.add("onErrorResumeNext: " + onErrorResumeNext());
        log.add("retry(2): " + retry());
        log.add("retryWhen con espera: " + retryWhen());
        log.add("cadena: " + chain());
        return log;
    }

    public static void main(String[] args) {
        run().forEach(System.out::println);
    }
}
