package dev.reactivelab.mutiny.examples;

import dev.reactivelab.domain.*;
import io.smallrye.mutiny.*;

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
        System.out.println(Uni.combine().all().unis(
                Uni.createFrom().item(() -> repo.findUser(1).orElseThrow().name()),
                Uni.createFrom().item(() -> repo.fetchStatus(100)))
                .asTuple().map(tuple -> tuple.getItem1() + ": " + tuple.getItem2())
                .await().indefinitely());
    }
}
