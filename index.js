"use strict";

const client = require("./lib/client");
const main = require("./lib/main");
const { patch, restore, isPatched } = require("./lib/patch");
const syncBridge = require("./lib/sync-bridge");

module.exports = {
  ...main,
  configure(opts) {
    client.configure(opts);
    if (opts && opts.defaultCwd) main._setDefaultCwd(opts.defaultCwd);
    // Enable WebSocket transport for spawn (streaming stdin + binary-safe).
    if (opts && opts.wsTransport) {
      main._setWsTransport(true, opts.baseURL, opts.token);
    } else if (opts && opts.wsTransport === false) {
      main._setWsTransport(false, null, null);
    }
  },
  patch() { patch(module.exports); },
  restore,
  isPatched,
  client,
  syncBridge,
  setDebugLogger: client.setDebugLogger,
  default: main,
};
