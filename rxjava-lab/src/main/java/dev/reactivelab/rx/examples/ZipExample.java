package dev.reactivelab.rx.examples;

import dev.reactivelab.domain.*;
import io.reactivex.rxjava3.core.*;

/**
 * 09: Combinar usuario y estado.
 *
 *   usuario --+--> resumen
 *   estado ----+
 *
 * 1. Se prepara la fuente y el operador indicado.
 * 2. La suscripción inicia el trabajo; el consumidor observa el resultado.
 * 3. Compara esta clase con su homónima del otro módulo.
 */
public final class ZipExample {
    private ZipExample() {}

    public static void main(String[] args) {
        var repo = new FakeRepository();
        System.out.println(Single.zip(
                Single.fromCallable(() -> repo.findUser(1).orElseThrow().name()),
                Single.fromCallable(() -> repo.fetchStatus(100)),
                (name, status) -> name + ": " + status).blockingGet());
    }
}
