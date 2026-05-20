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

        const source = document.getElementById(this.el.dataset.copySource)
        if (!source) return

        navigator.clipboard?.writeText(source.value)
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
  FavnTimeline: {
    mounted() {
      this.userPaused = false
      this.followNow()

      this.el.addEventListener("scroll", () => {
        if (this.ignoreScroll || this.el.dataset.liveFollow !== "true") return
        this.userPaused = true
        this.pushEvent("timeline_pause_live", {})
      }, {passive: true})

      const minimap = document.getElementById("run-timeline-minimap")
      minimap?.addEventListener("click", event => {
        const bounds = minimap.getBoundingClientRect()
        if (!bounds.width) return

        const ratio = Math.max(0, Math.min(1, (event.clientX - bounds.left) / bounds.width))
        this.el.scrollLeft = ratio * Math.max(this.el.scrollWidth - this.el.clientWidth, 0)
        if (this.el.dataset.liveFollow === "true") this.pushEvent("timeline_pause_live", {})
      })
    },
    updated() {
      this.followNow()
    },
    followNow() {
      if (this.el.dataset.active !== "true" || this.el.dataset.liveFollow !== "true") return
      if (this.el.dataset.fitMode === "true") return

      const offset = Number.parseFloat(this.el.dataset.nowOffset || "100") / 100
      const maxScroll = Math.max(this.el.scrollWidth - this.el.clientWidth, 0)
      const target = Math.max(0, Math.min(maxScroll, this.el.scrollWidth * offset - this.el.clientWidth * 0.72))

      this.ignoreScroll = true
      this.el.scrollLeft = target
      window.requestAnimationFrame(() => { this.ignoreScroll = false })
    }
  }
}

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
