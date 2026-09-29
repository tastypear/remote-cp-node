"use strict";

const Module = require("module");

let _originalCp = null;
let _originalValues = null;
let _patched = false;
let _origResolveFilename = null;
let _remoteCpRef = null;

function patch(remoteCp) {
  if (_patched) return;

  _originalCp = require("child_process");
  _originalValues = {};
  _remoteCpRef = remoteCp;

  for (const key of Object.keys(remoteCp)) {
    try {
      _originalValues[key] = _originalCp[key];
      Object.defineProperty(_originalCp, key, {
        value: remoteCp[key],
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