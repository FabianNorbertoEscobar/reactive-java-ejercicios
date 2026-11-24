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

`transformToUni` se demuestra en `FlatMapExample`; `transformToMulti` en
`TransformToMultiExample`. `runSubscriptionOn` y `emitOn` se ven juntos en
`SchedulersExample`. Los procesadores calientes no guardan un historial para
suscriptores tardíos; `PublishSubject` y `BroadcastProcessor` tampoco tienen
contratos idénticos de demanda. No emitas concurrentemente en un subject sin
considerar la serialización.

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

## Ejercicios propuestos

Implementa cada ejercicio en ambos módulos y compara resultados y errores:

1. Busca Luis y comprueba su nombre.
2. Busca un usuario inexistente sin convertir la ausencia en excepción.
3. Convierte nombres a mayúsculas.
4. Filtra productos por un precio configurable.
5. Calcula el total del pedido 101 con `BigDecimal`.
6. Cuenta los pedidos de cada usuario.
7. Añade un usuario sin pedidos y verifica el resumen vacío.
8. Simula un producto inexistente y verifica el error terminal.
9. Recupera exclusivamente errores de tipo conocido; propaga los demás.
10. Simula dos fallos y compara los resultados de uno y dos reintentos.
11. Introduce un límite temporal para una API lenta simulada.
12. Compara la salida de `merge` y `concat` con fuentes de distinta latencia.
13. Solicita eventos por lotes de dos y cancela antes de terminar.
14. Añade un consumidor tardío y compara fuente fría y caliente.
15. Comprueba que una segunda suscripción fría repite el trabajo.
16. Combina nombre y cantidad de pedidos en un DTO inmutable.
17. Añade una prueba que verifique el orden de productos del resumen.
18. Investiga cómo adaptar un `Flowable` a `Flux` sin perder demanda.
19. Crea un endpoint WebFlux que devuelva un `Mono` y otro con `Flux`.
20. Crea un endpoint Quarkus que devuelva un `Uni` y otro con `Multi`.

Los dos últimos son extensiones externas al laboratorio actual: crea proyectos
separados con sus dependencias y verifica que no bloquean el event loop.

## Entrevistas técnicas

- **¿En qué se diferencian asíncrono y reactivo?** Asíncrono describe cuándo se
  completa el trabajo; reactivo describe cómo se componen y consumen señales.
- **¿Qué activa un pipeline?** Normalmente la suscripción; algunas fuentes
  calientes pueden emitir independientemente de ella.
- **¿Qué diferencia `map` de `flatMap`?** `map` transforma un valor; `flatMap`
  encadena una fuente por valor y puede intercalar resultados.
- **¿Qué es backpressure?** La coordinación de demanda entre productor y
  consumidor; no elimina por sí sola el riesgo de sobrecarga externa.
- **¿Qué diferencia `merge` de `concat`?** `merge` puede intercalar fuentes;
  `concat` espera que termine una antes de suscribirse a la siguiente.
- **¿Una secuencia puede continuar tras un error terminal?** No; recuperación
  y reintento sustituyen o vuelven a suscribir la secuencia según el operador.
- **¿Qué diferencia una fuente fría de una caliente?** La fría suele ejecutar
  por suscriptor; la caliente puede compartir emisiones y perder eventos previos.
- **¿`subscribeOn` y `observeOn` hacen lo mismo?** No: el primero afecta la
  suscripción; el segundo desplaza las notificaciones posteriores.
- **¿Por qué no bloquear un event loop?** Porque impide atender otras tareas y
  degrada la concurrencia. Usa clientes no bloqueantes o desplaza el bloqueo.
- **¿Por qué un reintento puede ser peligroso?** Puede repetir efectos laterales;
  considera idempotencia, límites y esperas entre intentos.
- **¿Qué hacen `cancel` y `dispose`?** Solicitan detener la suscripción y
  liberar recursos; el trabajo ya iniciado puede necesitar cooperación.

## Referencias

- [Reactive Streams](https://www.reactive-streams.org/)
- [RxJava 3](https://github.com/ReactiveX/RxJava)
- [SmallRye Mutiny](https://smallrye.io/smallrye-mutiny/)
- [Project Reactor](https://projectreactor.io/)
- [Spring WebFlux](https://docs.spring.io/spring-framework/reference/web/webflux.html)
- [Quarkus y Mutiny](https://quarkus.io/guides/mutiny-primer)
