package dev.reactivelab.rx.examples;

import io.reactivex.rxjava3.subjects.PublishSubject;

/**
 * 17: Publicar eventos a dos consumidores.
 *
 *   productor -> subject/processor -> A y B
 *
 * 1. Se prepara la fuente y el operador indicado.
 * 2. La suscripción inicia el trabajo; el consumidor observa el resultado.
 * 3. Compara esta clase con su homónima del otro módulo.
 */
public final class SubjectsExample {
    private SubjectsExample() {}

    public static void main(String[] args) {
        var events = PublishSubject.<String>create();
        events.subscribe(item -> System.out.println("A: " + item));
        events.subscribe(item -> System.out.println("B: " + item));
        events.onNext("pedido creado");
        events.onComplete();
    }
}
