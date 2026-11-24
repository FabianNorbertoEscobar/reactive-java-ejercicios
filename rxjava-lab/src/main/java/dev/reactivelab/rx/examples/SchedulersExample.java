package dev.reactivelab.rx.examples;

import io.reactivex.rxjava3.core.*;
import io.reactivex.rxjava3.schedulers.Schedulers;

/**
 * 14: Cambiar el hilo de suscripción y de emisión.
 *
 *   consulta -- hilo I/O --> emisión -- otro hilo --> consumidor
 *
 * 1. Se prepara la fuente y el operador indicado.
 * 2. La suscripción inicia el trabajo; el consumidor observa el resultado.
 * 3. Compara esta clase con su homónima del otro módulo.
 */
public final class SchedulersExample {
    private SchedulersExample() {}

    public static void main(String[] args) {
        System.out.println(Single.fromCallable(() -> "Fuente: " + Thread.currentThread().getName())
                .subscribeOn(Schedulers.io()).observeOn(Schedulers.computation())
                .map(source -> source + ", consumidor: " + Thread.currentThread().getName()).blockingGet());
    }
}
