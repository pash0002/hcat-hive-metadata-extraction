package com.datametica.extraction.hive.util

import java.lang.RuntimeException
import java.security.Permission

class NoExitSecurityManager : SecurityManager() {
  override fun checkPermission(perm: Permission?) { // allow anything.
  }

  override fun checkPermission(perm: Permission?, context: Any?) { // allow anything.
  }

  override fun checkExit(status: Int) {
    super.checkExit(status)
    throw SecurityException("Raising exception, instead of System.exit(). Return code was: $status")
  }
}