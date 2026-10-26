// The few browser APIs the console uses, bound by hand: there is no binding library to keep
// in step, and every one of these is a single call.

type event
type mediaQueryList
type element

@val external window: {..} = "window"
@val external document: {..} = "document"

@val external addWindowListener: (string, event => unit) => unit = "window.addEventListener"
@val external removeWindowListener: (string, event => unit) => unit = "window.removeEventListener"
@val external addDocumentListener: (string, event => unit) => unit = "document.addEventListener"
@val
external removeDocumentListener: (string, event => unit) => unit = "document.removeEventListener"

@val external setInterval: (unit => unit, int) => int = "setInterval"
@val external clearInterval: int => unit = "clearInterval"
@val external setTimeout: (unit => unit, int) => int = "setTimeout"
@val external clearTimeout: int => unit = "clearTimeout"

@val external locationHash: string = "location.hash"
@val external locationPort: string = "location.port"
@val external locationProtocol: string = "location.protocol"
@set external setHash: ({..}, string) => unit = "hash"
let goToHash = hash => setHash(window["location"], hash)

@val external openWindow: (string, string) => unit = "window.open"

@val external matchMedia: string => mediaQueryList = "window.matchMedia"
@get external mediaMatches: mediaQueryList => bool = "matches"
@send external addMediaListener: (mediaQueryList, string, event => unit) => unit = "addEventListener"
@send
external removeMediaListener: (mediaQueryList, string, event => unit) => unit =
  "removeEventListener"

@val external now: unit => float = "Date.now"

// The <html> element's `data-theme`, which tokens.css keys the dark colours on.
@val external htmlDataset: {..} = "document.documentElement.dataset"
let setTheme = (theme: string) => htmlDataset["theme"] = theme

// `<meta name="…" content="…">`, for the two values the server fills into the page.
@val @return(nullable)
external querySelector: string => option<element> = "document.querySelector"
@get external content: element => string = "content"
let meta = name => querySelector(`meta[name="${name}"]`)->Option.map(content)

// Events, read without a typed record for each kind.
@get external eventKey: event => string = "key"
@get external eventNewValue: event => Nullable.t<string> = "newValue"
@get external eventStorageKey: event => Nullable.t<string> = "key"
@get external eventMetaKey: event => bool = "metaKey"
@get external eventCtrlKey: event => bool = "ctrlKey"
@get external eventAltKey: event => bool = "altKey"
@get external eventTarget: event => {..} = "target"
@send external preventDefault: event => unit = "preventDefault"

// Whether a key press landed in something the user types into, where `1`–`3` are text.
let typingIn = (event: event) => {
  let target = eventTarget(event)
  let tag: string = %raw(`(t) => (t && t.tagName) || ""`)(target)
  let editable: bool = %raw(`(t) => !!(t && t.isContentEditable)`)(target)
  tag == "INPUT" || tag == "TEXTAREA" || tag == "SELECT" || editable
}

// ⌘ on Apple machines, Ctrl elsewhere: the label on Run and the key that runs from anywhere.
let isMac: bool = %raw(`typeof navigator !== "undefined" && /Mac|iPhone|iPad/.test(navigator.platform || navigator.userAgent || "")`)
let runShortcut = isMac ? "⌘↵" : "Ctrl↵"
