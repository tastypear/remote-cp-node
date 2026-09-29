"use strict";

const client = require("./lib/client");
const main = require("./lib/main");
const { patch, restore, isPatched } = require("./lib/patch");

module.exports = {
  ...main,
  configure(opts) {
    client.configure(opts);
    if (opts && opts.defaultCwd) main._setDefaultCwd(opts.defaultCwd);
  },
  patch() { patch(module.exports); },
  restore,
  isPatched,
  client,
  default: main,
};
