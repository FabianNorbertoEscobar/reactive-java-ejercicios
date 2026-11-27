# Java Reactive Lab

Laboratorio práctico en **Java 21 y Maven** para aprender programación reactiva con
RxJava 3 y SmallRye Mutiny, y preparar el salto a Reactor, Spring WebFlux y Quarkus.
No necesitas un servidor, una base de datos ni credenciales: todos los datos y las
llamadas a APIs están simulados en memoria.

## Estructura

```text
reactive-java-ejercicios/
├── pom.xml                 # agregador y versiones compartidas
├── common-domain/          # User, Product, Order, repositorio, datos y Summary
├── rxjava-lab/             # RxJava 3: ejemplos y pruebas
└── mutiny-lab/             # Mutiny: mismos problemas y pruebas
```

En cada módulo, `src/main/java/dev/reactivelab/{rx|mutiny}/examples` contiene
clases independientes con `main`, comentarios y diagramas. Las clases homónimas
resuelven el mismo problema; sus paquetes usan nombres Java válidos en lugar de
directorios como `01-single`. El número de cada ejercicio aparece en su Javadoc.
`OrderSummaryExample` implementa un caso de uso completo en ambos módulos.

## ¿Qué es programación reactiva?

Es una forma de componer procesamiento alrededor de **eventos que llegan con el
tiempo**. Una fuente puede producir valores, completarse o fallar; los operadores
declaran transformaciones, filtros y coordinación. La suscripción conecta el
consumidor con la fuente. Declarar un pipeline normalmente no lo ejecuta.

No es sinónimo de asincronía ni de paralelismo: una fuente puede emitir
sincrónicamente en el mismo hilo. Tampoco convierte I/O bloqueante en no bloqueante
por sí sola. El laboratorio usa `blockingGet()` y `await()` **solo en los
programas de consola** para esperar su salida; no los uses en un event loop.

## Reactive Streams

Reactive Streams define un protocolo para intercambiar secuencias asíncronas con
demanda controlada y cancelación. Especifica el contrato, no una API de operadores.
RxJava `Flowable`, Reactor `Flux` y Mutiny `Multi` soportan backpressure; las
adaptaciones de tipos y versiones de interfaces pueden requerir adaptadores.
`Single`, `Maybe`, `Completable` y `Uni` tienen contratos propios y no son
simplemente `Publisher` equivalentes.

### Publisher

Es la fuente: cuando recibe una suscripción puede entregar elementos de acuerdo
con la demanda, terminar normalmente o comunicar un error terminal.

### Subscriber

Recibe la suscripción y señales: elementos, error o finalización. Tras una señal
terminal no deben llegar más elementos.

### Subscription

Representa la relación entre ambos. El consumidor usa `request(n)` para pedir
elementos y `cancel()` para dejar de recibirlos. La cancelación es una solicitud
de detener el trabajo, no una garantía de que ningún trabajo ya iniciado siga.

### Backpressure

Permite que el consumidor regule cuánto recibe mediante demanda. No significa
que cualquier productor externo pueda frenarse: si una fuente caliente no puede
esperar, habrá que diseñar explícitamente una política de buffer, descarte o
error. Un buffer infinito puede agotar memoria. `Observable` de RxJava no
implementa este protocolo; para flujos sensibles a demanda usa `Flowable`.

## ¿Por qué existe RxJava?

RxJava lleva ReactiveX al ecosistema Java con una colección amplia de tipos,
operadores y schedulers. Es útil para componer fuentes de eventos, operaciones
asíncronas y flujos existentes, con una comunidad y ecosistema maduros.

## ¿Por qué existe Mutiny?

Mutiny ofrece una API orientada a eventos, con `Uni` para un resultado y `Multi`
para una secuencia. Su estilo `onItem()` / `onFailure()` hace visibles las
reacciones a cada señal; es la API reactiva integrada de forma destacada en Quarkus.

## ¿Cuándo usar RxJava?

Cuando el proyecto ya usa RxJava, necesitas su ecosistema de operadores o integras
fuentes que ya lo exponen. Elige `Flowable` si necesitas demanda; `Observable`
puede ser suficiente para fuentes pequeñas o cuando la demanda no aplica.

## ¿Cuándo usar Mutiny?

Cuando trabajas con Quarkus y sus extensiones reactivas o prefieres su API
orientada a eventos. `Uni` resulta apropiado para un resultado asíncrono y `Multi`
para secuencias con demanda. La elección depende también de las bibliotecas de
tu aplicación, no de una ventaja universal.

## Relación con Spring WebFlux

Spring WebFlux utiliza principalmente **Reactor**: `Mono` representa cero o un
elemento y `Flux` una secuencia. Como orientación, `Single` se parece a un `Mono`
con valor obligatorio y `Observable` a un `Flux` en cardinalidad. No son
intercambiables sin adaptadores: `Mono` admite vacío y `Flux` respeta demanda,
mientras `Observable` no. `Flowable` se aproxima más a `Flux` en backpressure.
WebFlux puede integrarse con otros tipos mediante adaptadores, pero aquí no se
implementa un servidor Spring ni se incluye Reactor como dependencia.

## Relación con Quarkus

Quarkus integra Mutiny: `Uni` representa una respuesta o fallo, y `Multi` una
secuencia con backpressure. Muchas extensiones reactivas de Quarkus ofrecen estos
tipos. Este proyecto es Java puro: no inicia Quarkus ni necesita su runtime.

## Tabla de equivalencias

| Concepto | RxJava | Reactor | Mutiny |
|---|---|---|---|
| Un valor o error | `Single<T>` | `Mono<T>` (también puede estar vacío) | `Uni<T>` |
| Cero o un valor | `Maybe<T>` | `Mono<T>` | `Uni<T>` con `null` como ausencia convencional |
| Finalización sin valor | `Completable` | `Mono<Void>` | `Uni<Void>` con `null` |
| Secuencia sin protocolo de demanda | `Observable<T>` | Sin equivalente exacto | Sin equivalente exacto |
| Secuencia con demanda | `Flowable<T>` | `Flux<T>` | `Multi<T>` |
| Transformación síncrona | `map` | `map` | `onItem().transform` / `map` |
| Encadenar una operación | `flatMap` | `flatMap` | `onItem().transformToUni` |
| Filtrar | `filter` | `filter` | `select().where` |
| Combinar resultados | `zip` | `zip` | `Uni.combine().all().unis` |
| Recuperar un fallo | `onErrorReturnItem` | `onErrorReturn` | `onFailure().recoverWithItem` |
| Reintentar | `retry` | `retry` | `onFailure().retry()` |
| Hilo de suscripción | `subscribeOn` | `subscribeOn` | `runSubscriptionOn` |
| Hilo de señales posteriores | `observeOn` | `publishOn` | `emitOn` |
| Crear desde valores | `just` / `fromIterable` / `range(inicio, cantidad)` | `just` / `fromIterable` / `range` | `items` / `iterable` / `range(inicio, finExclusivo)` |
| Vacío / error | `empty` / `error` | `empty` / `error` | `empty` / `failure` |
| Emisión manual (frío) | `create` / `generate` | `create` / `generate` | `emitter` / `generator` |
| Transformación en serie | `concatMap` | `concatMap` | `transformToUniAndConcatenate` / `transformToMultiAndConcatenate` |
| Transformación concurrente | `flatMap(f, maxConcurrency)` | `flatMap` | `transformToUniAndMerge` / `transformToUni(...).merge(n)` |
| Acumular / agrupar | `scan` / `buffer` | `scan` / `buffer` | `onItem().scan` / `group().intoLists().of(n)` |
| Tomar / saltar | `take` / `skip` / `takeWhile` | `take` / `skip` / `takeWhile` | `select().first(n)` / `skip().first(n)` / `select().first(predicado)` |
| Sin repetidos | `distinct` | `distinct` | `select().distinct()` |
| Primero / último | `firstElement` / `lastOrError` | `next` / `last` | `collect().first()` / `select().last()` |
| Mezclar / concatenar | `merge` / `concat` | `merge` / `concat` | `Multi.createBy().merging()` / `concatenating()` |
| Último de cada fuente | `combineLatest` | `combineLatest` | `Multi.createBy().combining().streams(...).latestItems()` |
| Fuente caliente | `PublishSubject` | `Sinks.many().multicast()` | `BroadcastProcessor` |
| Con valor inicial | `BehaviorSubject` | `Sinks...replay().limit(1)` | `replaying().upTo(1).ofSeedAndMulti` |
| Con historial | `ReplaySubject` | `Sinks.many().replay()` | `Multi.createBy().replaying().ofMulti` |
| Solo el último al completar | `AsyncSubject` | — | `replaying().upTo(1).ofMulti(multi.select().last())` |
| Exceso de eventos | `onBackpressureBuffer/Drop/Latest` | `onBackpressureBuffer/Drop/Latest` | `onOverflow().buffer()/drop()/dropPreviousItems()` |
| Valor alternativo / fuente alternativa | `onErrorReturn` / `onErrorResumeNext` | `onErrorReturn` / `onErrorResume` | `onFailure().recoverWithItem` / `recoverWithMulti` |
| Reintento con política | `retryWhen` | `retryWhen` | `onFailure().retry().when(...)` / `withBackOff(...)` |

### Single vs Uni

| RxJava | Mutiny |
|---|---|
| `Single.fromCallable(() -> repo.findUser(1).orElseThrow())` | `Uni.createFrom().item(() -> repo.findUser(1).orElseThrow())` |
| `single.map(User::name)` | `uni.onItem().transform(User::name)` |

Ambos pueden producir un resultado o un error y permiten diferir el trabajo.
`Single` no admite `null`; `Uni` sí. Usa uno para una búsqueda cuyo resultado
debe existir, exponiendo el error si no existe.

### Maybe vs Uni

| RxJava | Mutiny |
|---|---|
| `Maybe.fromOptional(repo.findUser(999))` | `Uni.createFrom().item(() -> repo.findUser(999).orElse(null))` |

`Maybe` comunica ausencia como finalización sin elemento. `Uni` puede emitir
`null`, pero esa es una **convención de modelado**, no una señal de vacío
equivalente. Alternativamente puedes usar `Uni<Optional<User>>`. Úsalo para
búsquedas opcionales y trata explícitamente la ausencia.

### Observable vs Multi

| RxJava | Mutiny |
|---|---|
| `Observable.just(1, 2, 3).map(n -> n * 2)` | `Multi.createFrom().items(1, 2, 3).onItem().transform(n -> n * 2)` |

Ambos transforman secuencias, pero `Observable` **no** tiene demanda Reactive
Streams; `Multi` sí. Un `Multi` no puede emitir `null`. Elige según el contrato
de la fuente y el control necesario, no solo por la sintaxis.

### Flowable vs Multi

| RxJava | Mutiny |
|---|---|
| `Flowable.range(1, 5).filter(n -> n > 2)` | `Multi.createFrom().range(1, 6).select().where(n -> n > 2)` |

Ambos soportan demanda y cancelación. En el ejemplo, RxJava recibe inicio y
cantidad; el rango de Mutiny recibe inicio inclusivo y límite exclusivo. Sirven
para secuencias donde el consumidor debe controlar el ritmo, sujeto a las
capacidades reales de la fuente.

## Ruta de aprendizaje sugerida

1. Single: un resultado.
2. Maybe: resultado opcional.
3. Completable: finalización sin valor.
4. Observable: múltiples eventos.
5. Flowable: demanda.
6. map: transformación síncrona.
7. flatMap: composición de operaciones.
8. Error handling: recuperación y reintentos.
9. Schedulers: hilos de suscripción y notificación.
10. Backpressure: demanda y cancelación.
11. Reactor: relaciona lo aprendido con `Mono` y `Flux`.
12. Spring WebFlux: aplica Reactor a endpoints y clientes HTTP.

En paralelo compara cada ejemplo con Mutiny. Después de la ruta, estudia el
caso completo y practica con Quarkus. Los ejemplos no requieren instalar Reactor,
Spring ni Quarkus.

## Catálogo de ejercicios guiados

Abre en ambos módulos la clase indicada. Cada clase tiene su propio `main` y
diagrama; los pasos siguientes describen qué observar. El orden de `merge` y
los nombres de los hilos dependen de la ejecución.

| # | Clase | Paso a paso y resultado esperado |
|---|---|---|
| 1 | `SingleExample` | 1. Difiere la consulta del usuario 1. 2. Suscríbete. 3. Observa Ana. |
| 2 | `MaybeExample` | 1. Busca el id 999. 2. Modela la ausencia. 3. Imprime «No encontrado»; compara vacío y `null`. |
| 3 | `CompletableExample` | 1. Prepara la acción de confirmación. 2. Suscríbete. 3. Observa la finalización sin valor útil. |
| 4 | `ObservableExample` | 1. Crea tres eventos. 2. Suscríbete. 3. Observa creado, pagado y enviado. |
| 5 | `FlowableExample` | 1. Suscríbete con demanda inicial cero. 2. Pide dos. 3. Observa `[1, 2]` y cancela. |
| 6 | `MapExample` | 1. Emite usuarios. 2. Transforma cada usuario en su nombre. 3. Observa Ana y Luis. |
| 7 | `FlatMapExample` | 1. Busca Ana. 2. Encadena la búsqueda de sus pedidos. 3. Observa el pedido 100. |
| 8 | `FilterExample` | 1. Emite productos. 2. Selecciona precios de hasta 20. 3. Observa Ratón. |
| 9 | `ZipExample` | 1. Consulta nombre y estado. 2. Combina ambos resultados. 3. Observa `Ana: CONFIRMADO`. |
| 10 | `MergeExample` | 1. Crea dos fuentes. 2. Mézclalas. 3. Observa cuatro eventos sin asumir orden entre fuentes. |
| 11 | `ConcatExample` | 1. Crea las mismas fuentes. 2. Concatena. 3. Comprueba que B comienza después de A. |
| 12 | `RetryExample` | 1. Simula un fallo temporal. 2. Reintenta una vez. 3. Observa el estado y dos intentos. |
| 13 | `ErrorHandlingExample` | 1. Consulta un pedido inexistente. 2. Recupera el fallo. 3. Observa `DESCONOCIDO`. |
| 14 | `SchedulersExample` | 1. Desplaza la suscripción. 2. Desplaza la emisión. 3. Compara los nombres de los hilos. |
| 15 | `BackpressureExample` | 1. Suscríbete con demanda cero. 2. Pide tres de cien. 3. Observa `[1, 2, 3]` y cancela. |
| 16 | `HotColdExample` | 1. Suscribe dos consumidores a una fuente fría. 2. Publica en una caliente antes y después del segundo consumidor. 3. Observa qué eventos recibe cada uno. |
| 17 | `SubjectsExample` | 1. Conecta dos consumidores. 2. Publica un evento. 3. Ambos lo reciben; completa la fuente. |
| 18 | `TransformToMultiExample` | 1. Busca un pedido. 2. Aplana sus ids en una secuencia. 3. Consulta e imprime sus productos. |
| 19 | `MultiTransformationsExample` | 1. Emite ids. 2. Encadena consultas individuales en orden. 3. Transforma los productos a nombres. |
| 20 | `CreationOperatorsExample` | 1. Crea fuentes con `just/items`, `fromIterable/iterable`, `range`, `empty`, `error/failure` y `create/emitter`. 2. Recógelas en listas. 3. Observa `range: [1, 2, 3, 4, 5]` y la diferencia entre el segundo argumento de `range` en cada biblioteca. |
| 21 | `TransformationOperatorsExample` | 1. Aplica `map`, `flatMap`, `concatMap`, `scan`, `buffer` y `toMap`. 2. Con retardos distintos, `flatMap` entrega `[2, 1]` y `concatMap` `[1, 2]`. |
| 22 | `FilteringOperatorsExample` | 1. Aplica `filter`, `take`, `skip`, `distinct`, `first`, `last`, `takeWhile` y `skipWhile` a `1 2 2 3 4 5`. 2. Compara `Maybe`/`firstElement` con `collect().first()` (`null` si está vacío). |
| 23 | `CombiningOperatorsExample` | 1. `merge` entrega `[B1, A1]`; `concat`, `[A1, B1]`. 2. `zip` empareja por posición. 3. `combineLatest` produce `[1x, 2x, 2y]` con fuentes controladas a mano. |
| 24 | `ConcurrencyExample` | 1. Observa los hilos tras `subscribeOn/runSubscriptionOn` y `observeOn/emitOn`. 2. Dos tareas de 300 ms en paralelo tardan ~300 ms. 3. Recupera o propaga un fallo asíncrono. |
| 25 | `SubjectTypesExample` | 1. Publica con A suscrito y B tardío. 2. Compara `Publish`, `Behavior`, `Replay` y `Async` con `BroadcastProcessor` y `replaying()`. |
| 26 | `ManualEmissionExample` | 1. `create`/`emitter` es frío: se ejecuta por suscriptor. 2. Libera recursos al cancelar. 3. `generate`/`generator` emite bajo demanda; `UnicastProcessor` acumula hasta tener suscriptor. |
| 27 | `BackpressureStrategiesExample` | 1. Una fuente caliente sin demanda falla. 2. Compara `buffer`, `drop` y `latest` (`onOverflow` en Mutiny). 3. Una fuente fría respeta `request(n)`. |
| 28 | `ErrorOperatorsExample` | 1. `onError` termina el flujo tras `[10, 5]`. 2. Recupera con valor o fuente alternativa. 3. `retry`, `retryWhen` / `retry().when` y `withBackOff`. 4. Un fallo en una cadena llega al manejador final. |

`transformToUni` se demuestra en `FlatMapExample`; `transformToMulti` en
`TransformToMultiExample`. `runSubscriptionOn` y `emitOn` se ven juntos en
`SchedulersExample`. Los procesadores calientes no guardan un historial para
suscriptores tardíos; `PublishSubject` y `BroadcastProcessor` tampoco tienen
contratos idénticos de demanda. No emitas concurrentemente en un subject sin
considerar la serialización.

## Matriz de cobertura

Cada tema tiene una clase homónima en `rxjava-lab` y en `mutiny-lab`
(también hay una prueba por módulo en `ConceptExamplesTest`).

| Tema | Ejemplos | Estado |
|---|---|---|
| A. Tipos (`Observable`, `Single`, `Completable`, `Flowable` / `Multi`, `Uni`) | 1-5 y tablas de equivalencias | Completo |
| B. Creación | 20 | Completo |
| C. Transformación | 6, 7, 18, 19, 21 | Completo |
| D. Filtrado | 8, 22 | Completo |
| E. Combinación | 9, 10, 11, 23 | Completo |
| F. Asincronismo y concurrencia | 14, 24 | Completo |
| G. Subjects y emisión manual | 16, 17, 25, 26 | Completo |
| H. Backpressure | 5, 15, 27 | Completo |
| I. Errores | 12, 13, 28 | Completo |

Diferencias a tener presentes: Mutiny no tiene `Observable` (todo `Multi` soporta
demanda), ni `Maybe` (se usa `Uni` con `null` u `Optional`), ni schedulers propios
(usa `Executor` de Java), ni Subjects (usa procesadores o `replaying()`).
Los `main` de Mutiny que usan retardos cierran sus hilos por defecto con
`ExampleRuntime`, solo necesario en programas de consola.

## Caso de uso: resumen de pedidos

```text
buscarUsuario(id) -> buscarPedidos(usuario)
                     |-> buscarProductos(ids) --+
                     +-> API: estados ----------+-> generarResumen
```

Ejecuta `dev.reactivelab.rx.OrderSummaryExample` y
`dev.reactivelab.mutiny.OrderSummaryExample`. Ambos consultan los mismos datos:
Ana tiene un pedido con teclado y ratón por un total de `60.00`, con estado
`CONFIRMADO`. Un usuario desconocido produce un error; sin pedidos, el total es
cero y el estado es `SIN_PEDIDOS`. Los productos se consultan en orden y los
resultados de productos y estados se combinan. Los pools desplazan las consultas
simuladas síncronas; Mutiny usa el pool común porque los datos están en memoria,
no hay una API HTTP ni una base de datos real. En producción, prefiere clientes
no bloqueantes o usa un executor dedicado para I/O bloqueante, dimensionado y
cerrado de forma apropiada; no bloquees el pool común.

## Cómo ejecutar ejemplos

Requisitos: JDK 21 y Maven 3.9 o posterior. Desde la raíz:

```bash
java -version
mvn -version
mvn clean test
mvn install
```

`install` instala los tres artefactos locales para poder resolver
`common-domain` al ejecutar un módulo por separado. Ejemplos:

```bash
mvn -pl rxjava-lab exec:java -Dexec.mainClass=dev.reactivelab.rx.examples.SingleExample
mvn -pl mutiny-lab exec:java -Dexec.mainClass=dev.reactivelab.mutiny.examples.SingleExample
mvn -pl rxjava-lab exec:java -Dexec.mainClass=dev.reactivelab.rx.examples.BackpressureExample
mvn -pl mutiny-lab exec:java -Dexec.mainClass=dev.reactivelab.mutiny.examples.BackpressureExample
mvn -pl rxjava-lab exec:java -Dexec.mainClass=dev.reactivelab.rx.OrderSummaryExample
mvn -pl mutiny-lab exec:java -Dexec.mainClass=dev.reactivelab.mutiny.OrderSummaryExample
```

Sustituye `SingleExample` por cualquier clase del catálogo. Tras modificar
fuentes, vuelve a ejecutar `mvn install` antes de lanzar ejemplos. En un IDE,
importa el `pom.xml` raíz y ejecuta el `main` deseado.

## Testing

Las pruebas usan JUnit 5. RxJava ofrece `TestObserver` mediante `.test()`:

```java
Observable.just(1, 2, 3).filter(n -> n > 1).map(n -> n * 10)
    .test().assertResult(20, 30);
```

Mutiny ofrece `UniAssertSubscriber` y `AssertSubscriber`:

```java
Multi.createFrom().items(1, 2, 3).select().where(n -> n > 1)
    .onItem().transform(n -> n * 10)
    .subscribe().withSubscriber(AssertSubscriber.create(2))
    .assertCompleted().assertItems(20, 30);
```

Las pruebas del resumen verifican valor y propagación de errores con espera
acotada. Para flujos infinitos o asíncronos, solicita solo la demanda necesaria,
espera con límites y cancela recursos.

## Referencias

- [Reactive Streams](https://www.reactive-streams.org/)
- [RxJava 3](https://github.com/ReactiveX/RxJava)
- [SmallRye Mutiny](https://smallrye.io/smallrye-mutiny/)
- [Project Reactor](https://projectreactor.io/)
- [Spring WebFlux](https://docs.spring.io/spring-framework/reference/web/webflux.html)
- [Quarkus y Mutiny](https://quarkus.io/guides/mutiny-primer)
