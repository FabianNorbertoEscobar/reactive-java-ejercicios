package dev.reactivelab.mutiny.examples;

import dev.reactivelab.domain.*;
import io.smallrye.mutiny.*;

/**
 * 02: Buscar un usuario inexistente.
 *
 *   consulta -> usuario o ausencia
 *
 * 1. Se prepara la fuente y el operador indicado.
 * 2. La suscripción inicia el trabajo; el consumidor observa el resultado.
 * 3. Compara esta clase con su homónima del otro módulo.
 */
public final class MaybeExample {
    private MaybeExample() {}

    public static void main(String[] args) {
        var repo = new FakeRepository();
        Uni.createFrom().item(() -> repo.findUser(999).orElse(null))
                .subscribe().with(user -> System.out.println(user == null ? "No encontrado" : user),
                        Throwable::printStackTrace);
    }
}
