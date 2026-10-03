"use strict";

const Module = require("module");

let _originalCp = null;
let _originalValues = null;
let _patched = false;
let _origResolveFilename = null;
let _remoteCpRef = null;

// Check if a command should stay local (not routed to remote).
// When client.shouldRemote callback is set, returns !shouldRemote(method, cmd, args, opts).
// Default: false (everything goes remote).
function _shouldLocal(method, args) {
  const client = require("./client");
  const custom = client.shouldRemote;
  if (typeof custom === "function") return !custom(method, args[0], args[1], args[2]);
  return false;
}

function patch(remoteCp) {
  if (_patched) return;

  _originalCp = require("child_process");
  _originalValues = {};
  _remoteCpRef = remoteCp;

  for (const key of Object.keys(remoteCp)) {
    try {
      _originalValues[key] = _originalCp[key];
      const remoteFn = remoteCp[key];
      const origFn = _originalCp[key];

      let wrapped;
      if (typeof remoteFn === "function" && typeof origFn === "function") {
        wrapped = function (...args) {
          if (_shouldLocal(key, args)) return origFn.apply(this, args);
          return remoteFn.apply(this, args);
        };
      } else {
        wrapped = remoteFn;
      }

      Object.defineProperty(_originalCp, key, {
        value: wrapped,
        writable: true,
        enumerable: true,
        configurable: true,
      });
    } catch {}
  }

  // Intercept node:child_process
  const cpModulePath = require.resolve("child_process");
  const prevResolveFilename = Module._resolveFilename;
  _origResolveFilename = prevResolveFilename;
  Module._resolveFilename = function (request, parent, isMain, options) {
    if (request === "node:child_process") {
      return cpModulePath;
    }
    return prevResolveFilename.call(this, request, parent, isMain, options);
  };

  _patched = true;
}

function restore() {
  if (!_patched) return;

  if (_originalCp && _originalValues) {
    for (const key of Object.keys(_originalValues)) {
      try {
        Object.defineProperty(_originalCp, key, {
          value: _originalValues[key],
          writable: true,
          enumerable: true,
          configurable: true,
        });
      } catch {}
    }
  }

  if (_origResolveFilename) {
    Module._resolveFilename = _origResolveFilename;
    _origResolveFilename = null;
  }

  _patched = false;
  _originalCp = null;
  _originalValues = null;
  _remoteCpRef = null;
}

function isPatched() {
  return _patched;
}

module.exports = { patch, restore, isPatched };