/**
 * Copyright (C) 2013 Spotify AB
 */

package jsr166.concurrent;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class ReflectedUnsafe {

  @SuppressWarnings("restriction")
  public static Unsafe getUnsafe() {
    try {

      Field singleoneInstanceField = Unsafe.class.getDeclaredField("theUnsafe");
      singleoneInstanceField.setAccessible(true);
      return (Unsafe) singleoneInstanceField.get(null);

    } catch (IllegalArgumentException e) {
      throw createExceptionForObtainingUnsafe(e);
    } catch (SecurityException e) {
      throw createExceptionForObtainingUnsafe(e);
    } catch (NoSuchFieldException e) {
      throw createExceptionForObtainingUnsafe(e);
    } catch (IllegalAccessException e) {
      throw createExceptionForObtainingUnsafe(e);
    }
  }

  private static RuntimeException createExceptionForObtainingUnsafe(final Throwable cause) {
    return new RuntimeException("error while obtaining sun.misc.Unsafe", cause);
  }
}
