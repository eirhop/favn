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
      this.scrollToBottom()
      this.el.addEventListener("click", event => {
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
    scrollToBottom() {
      if (this.el.dataset.liveTail !== "true") return

      const terminal = this.el.querySelector("[data-testid='log-terminal-window']")
      if (terminal) terminal.scrollTop = terminal.scrollHeight
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
  FavnTimeline: {
    mounted() {
      this.userPaused = false
      this.followNow()
      this.syncMinimapViewport()

      this.el.addEventListener("scroll", () => {
        if (!this.ignoreScroll && this.el.dataset.liveFollow === "true") {
          this.userPaused = true
          this.pushEvent("timeline_pause_live", {})
        }

        this.syncMinimapViewport()
      }, {passive: true})

      const minimap = document.getElementById("run-timeline-minimap")
      minimap?.addEventListener("click", event => {
        const track = minimap.querySelector("[data-testid='timeline-minimap-track']") || minimap
        const bounds = track.getBoundingClientRect()
        if (!bounds.width) return

        const ratio = Math.max(0, Math.min(1, (event.clientX - bounds.left) / bounds.width))
        this.pendingFocusRatio = ratio
        this.pushEvent("timeline_focus", {ratio})
        this.scrollToRatio(ratio)
      })
    },
    updated() {
      this.followNow()
      this.syncMinimapViewport()
      if (this.pendingFocusRatio !== undefined) {
        const ratio = this.pendingFocusRatio
        this.pendingFocusRatio = undefined
        window.requestAnimationFrame(() => {
          this.scrollToRatio(ratio)
          this.syncMinimapViewport()
        })
      }
    },
    scrollToRatio(ratio) {
      const maxScroll = Math.max(this.el.scrollWidth - this.el.clientWidth, 0)
      this.ignoreScroll = true
      this.el.scrollLeft = Math.max(0, Math.min(maxScroll, this.el.scrollWidth * ratio - this.el.clientWidth / 2))
      window.requestAnimationFrame(() => {
        this.ignoreScroll = false
        this.syncMinimapViewport()
      })
    },
    followNow() {
      if (this.el.dataset.active !== "true" || this.el.dataset.liveFollow !== "true") return
      if (this.el.dataset.fitMode === "true") return

      const offset = Number.parseFloat(this.el.dataset.nowOffset || "100") / 100
      const maxScroll = Math.max(this.el.scrollWidth - this.el.clientWidth, 0)
      const target = Math.max(0, Math.min(maxScroll, this.el.scrollWidth * offset - this.el.clientWidth * 0.72))

      this.ignoreScroll = true
      this.el.scrollLeft = target
      window.requestAnimationFrame(() => {
        this.ignoreScroll = false
        this.syncMinimapViewport()
      })
    },
    syncMinimapViewport() {
      const minimap = document.getElementById("run-timeline-minimap")
      const viewport = minimap?.querySelector("[data-testid='timeline-minimap-viewport']")
      if (!viewport) return

      const scrollWidth = Math.max(this.el.scrollWidth, this.el.clientWidth, 1)
      const width = Math.max(0, Math.min(100, this.el.clientWidth / scrollWidth * 100))
      const left = Math.max(0, Math.min(100 - width, this.el.scrollLeft / scrollWidth * 100))

      viewport.style.left = `${left}%`
      viewport.style.width = `${width}%`
    }
  }
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
