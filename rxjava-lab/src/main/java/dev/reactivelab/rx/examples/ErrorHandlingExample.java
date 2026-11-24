package dev.reactivelab.rx.examples;

import dev.reactivelab.domain.*;
import io.reactivex.rxjava3.core.*;

/**
 * 13: Recuperar un fallo de API.
 *
 *   API -> error -> valor alternativo
 *
 * 1. Se prepara la fuente y el operador indicado.
 * 2. La suscripción inicia el trabajo; el consumidor observa el resultado.
 * 3. Compara esta clase con su homónima del otro módulo.
 */
public final class ErrorHandlingExample {
    private ErrorHandlingExample() {}

    public static void main(String[] args) {
        var repo = new FakeRepository();
        System.out.println(Single.fromCallable(() -> repo.fetchStatus(999))
                .onErrorReturnItem("DESCONOCIDO").blockingGet());
    }
}
