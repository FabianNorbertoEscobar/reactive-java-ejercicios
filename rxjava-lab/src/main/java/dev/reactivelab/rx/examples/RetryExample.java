package dev.reactivelab.rx.examples;

import dev.reactivelab.domain.*;
import io.reactivex.rxjava3.core.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 12: Reintentar una API que falla una vez.
 *
 *   intento 1 -> error -> intento 2 -> OK
 *
 * 1. Se prepara la fuente y el operador indicado.
 * 2. La suscripción inicia el trabajo; el consumidor observa el resultado.
 * 3. Compara esta clase con su homónima del otro módulo.
 */
public final class RetryExample {
    private RetryExample() {}

    public static void main(String[] args) {
        var repo = new FakeRepository();
        var attempts = new AtomicInteger();
        System.out.println(Single.fromCallable(() -> {
            if (attempts.incrementAndGet() == 1) throw new IllegalStateException("API temporalmente caída");
            return repo.fetchStatus(100);
        }).retry(1).blockingGet());
        System.out.println("Intentos: " + attempts.get());
    }
}
