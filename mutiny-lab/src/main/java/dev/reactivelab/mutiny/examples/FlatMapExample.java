package dev.reactivelab.mutiny.examples;

import dev.reactivelab.domain.*;
import io.smallrye.mutiny.*;

/**
 * 07: Buscar pedidos de un usuario.
 *
 *   usuario -> consulta asíncrona -> pedidos
 *
 * 1. Se prepara la fuente y el operador indicado.
 * 2. La suscripción inicia el trabajo; el consumidor observa el resultado.
 * 3. Compara esta clase con su homónima del otro módulo.
 */
public final class FlatMapExample {
    private FlatMapExample() {}

    public static void main(String[] args) {
        var repo = new FakeRepository();
        Uni.createFrom().item(() -> repo.findUser(1).orElseThrow())
                .onItem().transformToUni(user -> Uni.createFrom().item(() -> repo.findOrders(user.id())))
                .await().indefinitely().forEach(System.out::println);
    }
}
