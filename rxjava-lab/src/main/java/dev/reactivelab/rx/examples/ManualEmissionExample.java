package dev.reactivelab.rx.examples;

import io.reactivex.rxjava3.core.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 26: Emisión manual de eventos con create: ObservableEmitter, FlowableEmitter, Single.create y generate.
 *
 *   callback/listener --emitter.onNext--> Observable/Flowable --> suscriptor
 *
 * Diferencia clave con un Subject: {@code create} es FRÍO. El lambda se ejecuta de nuevo
 * por cada suscriptor y cada uno recibe su propia secuencia. Un Subject es CALIENTE y
 * comparte una única emisión entre todos.
 *
 * 1. Dos suscriptores a un Observable.create: el productor se ejecuta dos veces.
 * 2. Un emisor se cancela: setCancellable libera el recurso (por ejemplo, quitar un listener).
 * 3. Flowable.create exige una estrategia de backpressure; Flowable.generate emite bajo demanda.
 * 4. Compara con {@code ManualEmissionExample} del módulo Mutiny.
 *
 * Resultado esperado:
 *   create frío: ejecuciones=2 A=[1, 2] B=[1, 2]
 *   cancelación: [1] liberado=true
 *   Flowable.create BUFFER: [1, 2, 3]
 *   Single.create: resultado
 *   Flowable.generate: [1, 2, 3, 4]
 */
public final class ManualEmissionExample {
    private ManualEmissionExample() {}

    public static String coldCreate() {
        var executions = new AtomicInteger();
        var source = Observable.<Integer>create(emitter -> {
            executions.incrementAndGet();
            emitter.onNext(1);
            emitter.onNext(2);
            emitter.onComplete();
        });
        var a = source.toList().blockingGet();
        var b = source.toList().blockingGet();
        return "ejecuciones=" + executions.get() + " A=" + a + " B=" + b;
    }

    public static String cancellation() {
        var released = new AtomicInteger();
        var received = new ArrayList<Integer>();
        Observable.<Integer>create(emitter -> {
            emitter.setCancellable(released::incrementAndGet); // se invoca al cancelar o terminar
            emitter.onNext(1);
            // No se completa: simula un listener abierto hasta que el consumidor se va.
        }).subscribe(received::add).dispose();
        return received + " liberado=" + (released.get() == 1);
    }

    public static List<Integer> flowableCreate() {
        return Flowable.<Integer>create(emitter -> {
            for (int i = 1; i <= 3 && !emitter.isCancelled(); i++) emitter.onNext(i);
            emitter.onComplete();
        }, BackpressureStrategy.BUFFER).toList().blockingGet();
    }

    public static String singleCreate() {
        return Single.<String>create(emitter -> emitter.onSuccess("resultado")).blockingGet();
    }

    /** generate emite un elemento por llamada y solo cuando hay demanda. */
    public static List<Integer> flowableGenerate() {
        return Flowable.generate(() -> 1, (state, emitter) -> {
            emitter.onNext(state);
            if (state == 4) emitter.onComplete();
            return state + 1;
        }).map(n -> (Integer) n).toList().blockingGet();
    }

    public static List<String> run() {
        var log = new ArrayList<String>();
        log.add("create frío: " + coldCreate());
        log.add("cancelación: " + cancellation());
        log.add("Flowable.create BUFFER: " + flowableCreate());
        log.add("Single.create: " + singleCreate());
        log.add("Flowable.generate: " + flowableGenerate());
        return log;
    }

    public static void main(String[] args) {
        run().forEach(System.out::println);
    }
}
