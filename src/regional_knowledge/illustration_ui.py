"""Inline MCP Apps illustration viewer; the authorized tool remains the only image source."""

ILLUSTRATION_UI_URI = "ui://rkb/illustration/v1.html"
ILLUSTRATION_UI_MIME = "text/html;profile=mcp-app"
ILLUSTRATION_UI_META = {
    "ui": {
        "prefersBorder": True,
        "csp": {"connectDomains": [], "resourceDomains": [], "frameDomains": []},
    },
}

# No external scripts, styles, URLs, or additional image-reading transport.
ILLUSTRATION_UI_HTML = r"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Иллюстрация из книги</title>
  <style>
    :root { color-scheme: light dark; font-family: system-ui, sans-serif; }
    body { margin: 0; padding: 8px; background: transparent; }
    figure { margin: 0; display: grid; gap: 8px; }
    img { display: none; width: 100%; max-height: 760px; height: auto; object-fit: contain; }
    figcaption { color: CanvasText; font-size: 13px; white-space: pre-line; }
  </style>
</head>
<body>
  <figure>
    <img id="picture" alt="Иллюстрация из книги">
    <figcaption id="caption">Ожидание иллюстрации…</figcaption>
  </figure>
  <script>
  (() => {
    "use strict";
    const image = document.getElementById("picture");
    const caption = document.getElementById("caption");
    const SAFE_MIME = /^image\/(?:jpeg|png|webp|gif|avif)$/i;
    const BASE64 = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;
    const INIT_ID = "rkb-illustration-init-v1";
    let initialized = false;
    let rendered = false;

    function object(value) {
      return value && typeof value === "object" && !Array.isArray(value) ? value : null;
    }

    function sizeChanged() {
      if (!initialized) return;
      requestAnimationFrame(() => {
        window.parent.postMessage({
          jsonrpc: "2.0", method: "ui/notifications/size-changed",
          params: {
            width: Math.ceil(window.innerWidth),
            height: Math.ceil(document.documentElement.scrollHeight),
          },
        }, "*");
      });
    }

    function render(raw) {
      const result = object(raw);
      const items = result && Array.isArray(result.content) ? result.content : [];
      const index = items.findIndex((value) => {
        const item = object(value);
        return item && item.type === "image" && typeof item.mimeType === "string" &&
          SAFE_MIME.test(item.mimeType) && typeof item.data === "string" &&
          item.data.length > 0 && item.data.length <= 67000000 &&
          item.data.length % 4 === 0 && BASE64.test(item.data);
      });
      if (index < 0) return false;
      const item = items[index];
      image.addEventListener("load", sizeChanged, { once: true });
      image.src = "data:" + item.mimeType.toLowerCase() + ";base64," + item.data;
      image.style.display = "block";
      const credit = items.slice(index + 1).find(
        (value) => object(value) && value.type === "text" && typeof value.text === "string"
      );
      caption.textContent = credit && credit.text.trim()
        ? credit.text : "Иллюстрация без подписи.";
      rendered = true;
      sizeChanged();
      return true;
    }

    function fallback(event) {
      const detail = object(event && event.detail);
      const globals = detail && object(detail.globals);
      const metadata = (globals && object(globals.toolResponseMetadata)) ||
        object(window.openai && window.openai.toolResponseMetadata);
      if (metadata && !render(metadata.mcp_tool_result) &&
          !render(metadata.call_tool_result) && !rendered) {
        caption.textContent = "Иллюстрация недоступна.";
      }
    }

    window.addEventListener("message", (event) => {
      if (event.source !== window.parent) return;
      const message = object(event.data);
      if (!message || message.jsonrpc !== "2.0") return;
      if (message.id === INIT_ID && message.method === undefined && !message.error) {
        initialized = true;
        window.parent.postMessage({
          jsonrpc: "2.0", method: "ui/notifications/initialized", params: {},
        }, "*");
        sizeChanged();
      } else if (message.method === "ui/notifications/tool-result") {
        if (!render(message.params) && !rendered) {
          caption.textContent = "Иллюстрация недоступна.";
          sizeChanged();
        }
      }
    }, { passive: true });

    window.parent.postMessage({
      jsonrpc: "2.0", id: INIT_ID, method: "ui/initialize",
      params: {
        appInfo: { name: "rkb-illustration-viewer", version: "1.0.0" },
        appCapabilities: { availableDisplayModes: ["inline"] },
        protocolVersion: "2026-01-26",
      },
    }, "*");
    window.addEventListener("openai:set_globals", fallback, { passive: true });
    window.addEventListener("resize", sizeChanged, { passive: true });
    fallback();
  })();
  </script>
</body>
</html>"""
