const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const {webcrypto} = require('node:crypto');
const source = fs.readFileSync(require('node:path').join(__dirname, '../apps/favn_view/assets/js/app.js'), 'utf8');
const registry = source.slice(source.indexOf('const operatorCommandRegistryKey'), source.indexOf('const Hooks ='));
const stored = new Map();
const documentEvents = new Map();
const windowEvents = new Map();
const localStorage = {getItem: key => stored.get(key) ?? null, setItem: (key, value) => stored.set(key, value)};
const document = {body: {dataset: {operatorCommandScope: 'workspace:actor'}},
  addEventListener: (name, handler) => documentEvents.set(name, handler),
  createElement: () => ({dataset: {}})};
const window = {addEventListener: (name, handler) => windowEvents.set(name, handler), alert: message => assert.fail(message)};
const context = vm.createContext({document, window, localStorage, crypto: webcrypto});
vm.runInContext(registry, context);
const form = {dataset: {commandOperation: 'pipeline_backfill_submit', commandResource: 'pipeline'},
  elements: {namedItem: () => null}, closest() {return this},
  querySelector() {return this.input}, appendChild(input) {this.input = input}};
function submit() {
  documentEvents.get('submit')({target: form, preventDefault: () => assert.fail('blocked'), stopImmediatePropagation() {}});
  return form.input.value;
}
const first = submit();
assert.equal(submit(), first, 'unknown attempt must retain key');
// Reinitialize the actual event handlers to represent a reconnect/reload.
vm.runInContext(registry, vm.createContext({document, window, localStorage, crypto: webcrypto}));
assert.equal(submit(), first, 'reload must retain unresolved key');
windowEvents.get('phx:operator-command-terminal')({detail: {idempotency_key: first}});
const corrected = submit();
assert.notEqual(corrected, first, 'terminal acknowledgement must release the same slot');
assert.equal(submit(), corrected, 'new unresolved command must retain its new key');
console.log('PASS: actual browser registry retains unresolved keys across reload and releases terminal keys for corrected submissions');
