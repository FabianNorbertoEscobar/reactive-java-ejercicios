package dev.reactivelab.rx.examples;

import io.reactivex.rxjava3.core.*;
import io.reactivex.rxjava3.subjects.PublishSubject;

/**
 * 16: Comparar fuentes frías y calientes.
 *
 *   fría: cada suscriptor reinicia
 *   caliente: tardío solo ve eventos futuros
 *
 * 1. Se prepara la fuente y el operador indicado.
 * 2. La suscripción inicia el trabajo; el consumidor observa el resultado.
 * 3. Compara esta clase con su homónima del otro módulo.
 */
public final class HotColdExample {
    private HotColdExample() {}

    public static void main(String[] args) {
        var cold = Observable.defer(() -> Observable.just("inicio", "fin"));
        cold.subscribe(item -> System.out.println("frío A: " + item));
        cold.subscribe(item -> System.out.println("frío B: " + item));
        var hot = PublishSubject.<String>create();
        hot.subscribe(item -> System.out.println("caliente A: " + item));
        hot.onNext("inicio");
        hot.subscribe(item -> System.out.println("caliente B: " + item));
        hot.onNext("fin");
        hot.onComplete();
    }
}
