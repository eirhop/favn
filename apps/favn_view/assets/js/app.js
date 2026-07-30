// If you want to use Phoenix channels, run `mix help phx.gen.channel`
// to get started and then uncomment the line below.
// import "./user_socket.js"

// You can include dependencies in two ways.
//
// The simplest option is to put them in assets/vendor and
// import them using relative paths:
//
//     import "../vendor/some-package.js"
//
// Alternatively, you can `npm install some-package --prefix assets` and import
// them using a path starting with the package name:
//
//     import "some-package"
//
// If you have dependencies that try to import CSS, esbuild will generate a separate `app.css` file.
// To load it, simply add a second `<link>` to your `root.html.heex` file.

// Include phoenix_html to handle method=PUT/DELETE in forms and buttons.
import "phoenix_html"
// Establish Phoenix Socket and LiveView configuration.
import {Socket} from "phoenix"
import {LiveSocket} from "phoenix_live_view"
import {hooks as colocatedHooks} from "phoenix-colocated/favn_view"
import topbar from "../vendor/topbar"

const themeKey = "favn:theme"
const defaultTheme = "favn-dark"
const allowedThemes = new Set(["favn-dark", "favn-light"])
const setTheme = theme => {
  const nextTheme = allowedThemes.has(theme) ? theme : defaultTheme
  localStorage.setItem(themeKey, nextTheme)
  document.documentElement.setAttribute("data-theme", nextTheme)
}

setTheme(localStorage.getItem(themeKey) || defaultTheme)
window.addEventListener("storage", event => event.key === themeKey && setTheme(event.newValue))
window.addEventListener("favn:set-theme", event => setTheme(event.target.dataset.favnTheme))

const operatorCommandRegistryKey = "favn:operator-command-intents:v1"

const readOperatorCommands = () => {
  const stored = JSON.parse(localStorage.getItem(operatorCommandRegistryKey) || "{}")

  return Object.fromEntries(
    Object.entries(stored).filter(([_slot, command]) =>
      command &&
      typeof command.key === "string" &&
      Number.isFinite(command.createdAt)
    )
  )
}

const writeOperatorCommands = commands => {
  localStorage.setItem(operatorCommandRegistryKey, JSON.stringify(commands))
}

const operatorCommandOperation = element => {
  const field = element.dataset.commandOperationPresentField
  const input = field && element.elements?.namedItem(field)

  if (input?.value && element.dataset.commandOperationPresent) {
    return element.dataset.commandOperationPresent
  }

  return element.dataset.commandOperation
}

const operatorCommandSlot = element => {
  const scope = document.body.dataset.operatorCommandScope
  const operation = operatorCommandOperation(element)
  const resourceField = element.dataset.commandResourceField
  const resourceInput = resourceField && element.elements?.namedItem(resourceField)
  const resource = resourceInput?.value || element.dataset.commandResource

  if (!scope || !operation || !resource) return null
  return JSON.stringify([scope, operation, resource])
}

const randomOperatorCommandKey = operation => {
  const bytes = new Uint8Array(16)
  crypto.getRandomValues(bytes)
  const random = Array.from(bytes, byte => byte.toString(16).padStart(2, "0")).join("")
  return `${operation}:browser:${random}`
}

const prepareOperatorCommand = element => {
  const slot = operatorCommandSlot(element)
  if (!slot) return null

  const commands = readOperatorCommands()
  const existing = commands[slot]
  if (existing) return existing.key

  const operation = operatorCommandOperation(element).replace(/[^a-zA-Z0-9_.-]/g, "_")
  const key = randomOperatorCommandKey(operation)
  commands[slot] = {key, createdAt: Date.now()}
  writeOperatorCommands(commands)
  return key
}

const blockUnsafeCommand = event => {
  event.preventDefault()
  event.stopImmediatePropagation()
  window.alert("This command could not be safely prepared. Enable browser storage and try again.")
}

// Prepare the durable key synchronously before LiveView serializes the event.
// The storage slot deliberately matches the server's one-pending-command
// boundary: actor/workspace + operation + resource. Keeping the actor scope
// stable across session rotation lets an exact unknown command be recovered.
// A changed request still conflicts instead of silently repeating a write.
document.addEventListener("submit", event => {
  const form = event.target.closest("form[data-command-operation]")
  if (!form) return

  try {
    const key = prepareOperatorCommand(form)
    if (!key) return blockUnsafeCommand(event)

    let input = form.querySelector("input[data-operator-command-key]")
    if (!input) {
      input = document.createElement("input")
      input.type = "hidden"
      input.name = "idempotency_key"
      input.dataset.operatorCommandKey = "true"
      form.appendChild(input)
    }
    input.value = key
  } catch (_error) {
    blockUnsafeCommand(event)
  }
}, true)

document.addEventListener("click", event => {
  const control = event.target.closest("[data-command-operation][phx-click]")
  if (!control) return

  try {
    const key = prepareOperatorCommand(control)
    if (!key) return blockUnsafeCommand(event)
    control.setAttribute("phx-value-idempotency_key", key)
  } catch (_error) {
    blockUnsafeCommand(event)
  }
}, true)

window.addEventListener("phx:operator-command-terminal", event => {
  try {
    const key = event.detail?.idempotency_key
    if (typeof key !== "string") return

    const commands = readOperatorCommands()
    const remaining = Object.fromEntries(
      Object.entries(commands).filter(([_slot, command]) => command.key !== key)
    )
    writeOperatorCommands(remaining)
  } catch (_error) {
    // A failed acknowledgement is safe: the next action replays the exact key.
  }
})

const Hooks = {
  FavnClipboard: {
    mounted() {
      this.el.addEventListener("click", event => {
        const button = event.target.closest("[data-copy-text]")
        if (!button) return

        navigator.clipboard?.writeText(button.dataset.copyText || "")
      })
    }
  },
  FavnLogViewer: {
    mounted() {
      // Follow pauses while the operator reads scrollback and resumes when
      // they return to the bottom, so live tail never fights error-jumping.
      this.atBottom = true
      this.errorIndex = -1
      this.scrollToBottom()

      this.terminal()?.addEventListener("scroll", () => {
        const terminal = this.terminal()
        if (!terminal) return
        this.atBottom =
          terminal.scrollTop + terminal.clientHeight >= terminal.scrollHeight - 8
      })

      this.el.addEventListener("click", event => {
        const errorNav = event.target.closest("[data-log-error-nav]")
        if (errorNav) {
          this.jumpToError(errorNav.dataset.logErrorNav)
          return
        }

        const textButton = event.target.closest("[data-copy-text]")
        if (textButton) {
          navigator.clipboard?.writeText(textButton.dataset.copyText || "")
          return
        }

        const button = event.target.closest("[data-copy-logs]")
        if (!button) return

        const rows = Array.from(this.el.querySelectorAll("[data-log-copy-row]"))
        const text = rows.map(row => row.dataset.logCopyText || "").filter(Boolean).join("\n\n")
        navigator.clipboard?.writeText(text)
      })
    },
    updated() {
      this.scrollToBottom()
    },
    terminal() {
      return this.el.querySelector("[data-testid='log-terminal-window']")
    },
    scrollToBottom() {
      if (this.el.dataset.liveTail !== "true") return
      if (!this.atBottom) return

      const terminal = this.terminal()
      if (terminal) terminal.scrollTop = terminal.scrollHeight
    },
    jumpToError(direction) {
      const errors = Array.from(this.el.querySelectorAll("[data-log-level='error']"))
      if (errors.length === 0) return

      this.errorIndex = direction === "prev"
        ? (this.errorIndex <= 0 ? errors.length - 1 : this.errorIndex - 1)
        : (this.errorIndex >= errors.length - 1 ? 0 : this.errorIndex + 1)

      this.atBottom = false
      errors[this.errorIndex].scrollIntoView({block: "center"})
    }
  },
  LineageCanvas: {
    mounted() {
      this.scale = Number.parseFloat(this.el.dataset.zoom || "62") / 100
      this.pan = {x: 0, y: 0}
      this.drag = null
      this.content = this.el.querySelector(".lineage-canvas-content")
      this.applyTransform()

      this.el.addEventListener("pointerdown", event => {
        if (event.target.closest("button,a,input,select,textarea")) return
        this.drag = {x: event.clientX, y: event.clientY, pan: {...this.pan}}
        this.el.setPointerCapture(event.pointerId)
      })

      this.el.addEventListener("pointermove", event => {
        if (!this.drag) return
        this.pan = {
          x: this.drag.pan.x + event.clientX - this.drag.x,
          y: this.drag.pan.y + event.clientY - this.drag.y,
        }
        this.applyTransform()
      })

      this.el.addEventListener("pointerup", event => {
        this.drag = null
        if (this.el.hasPointerCapture(event.pointerId)) this.el.releasePointerCapture(event.pointerId)
      })

      this.el.addEventListener("wheel", event => {
        event.preventDefault()
        const delta = event.deltaY > 0 ? -0.06 : 0.06
        this.scale = Math.max(0.35, Math.min(1.4, this.scale + delta))
        this.applyTransform()
      }, {passive: false})
    },
    updated() {
      this.scale = Number.parseFloat(this.el.dataset.zoom || String(this.scale * 100)) / 100
      this.content = this.el.querySelector(".lineage-canvas-content")
      this.applyTransform()
    },
    applyTransform() {
      if (!this.content) return
      this.content.style.transform = `translate(${this.pan.x}px, ${this.pan.y}px) scale(${this.scale})`
    }
  },
}

document.addEventListener("click", event => {
  const button = event.target.closest("[data-copy-text]")
  if (!button) return

  navigator.clipboard?.writeText(button.dataset.copyText || "")
})

const csrfToken = document.querySelector("meta[name='csrf-token']").getAttribute("content")
const liveSocket = new LiveSocket("/live", Socket, {
  longPollFallbackMs: 2500,
  params: {_csrf_token: csrfToken},
  hooks: {...colocatedHooks, ...Hooks},
})

// Show progress bar on live navigation and form submits
topbar.config({barColors: {0: "#29d"}, shadowColor: "rgba(0, 0, 0, .3)"})
window.addEventListener("phx:page-loading-start", _info => topbar.show(300))
window.addEventListener("phx:page-loading-stop", _info => topbar.hide())

// connect if there are any LiveViews on the page
liveSocket.connect()

// expose liveSocket on window for web console debug logs and latency simulation:
// >> liveSocket.enableDebug()
// >> liveSocket.enableLatencySim(1000)  // enabled for duration of browser session
// >> liveSocket.disableLatencySim()
window.liveSocket = liveSocket

// The lines below enable quality of life phoenix_live_reload
// development features:
//
//     1. stream server logs to the browser console
//     2. click on elements to jump to their definitions in your code editor
//
if (process.env.NODE_ENV === "development") {
  window.addEventListener("phx:live_reload:attached", ({detail: reloader}) => {
    // Enable server log streaming to client.
    // Disable with reloader.disableServerLogs()
    reloader.enableServerLogs()

    // Open configured PLUG_EDITOR at file:line of the clicked element's HEEx component
    //
    //   * click with "c" key pressed to open at caller location
    //   * click with "d" key pressed to open at function component definition location
    let keyDown
    window.addEventListener("keydown", e => keyDown = e.key)
    window.addEventListener("keyup", _e => keyDown = null)
    window.addEventListener("click", e => {
      if(keyDown === "c"){
        e.preventDefault()
        e.stopImmediatePropagation()
        reloader.openEditorAtCaller(e.target)
      } else if(keyDown === "d"){
        e.preventDefault()
        e.stopImmediatePropagation()
        reloader.openEditorAtDef(e.target)
      }
    }, true)

    window.liveReloader = reloader
  })
}
