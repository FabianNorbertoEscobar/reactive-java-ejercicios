package dev.reactivelab.domain;

import java.math.BigDecimal;

/** Precio expresado en unidades monetarias, sin errores de redondeo binario. */
public record Product(long id, String name, BigDecimal price) {}
