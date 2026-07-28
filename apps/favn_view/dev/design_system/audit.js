// The design-system audit, served only by the development server.
//
// One call, `window.favn.audit()`, returns everything an agent needs to judge a
// rendered component: measured colours with contrast verdicts, control sizes,
// accessible names, clipping, and the bounding box of every example. The boxes
// ride along with the verdicts, so cropping a screenshot to one component
// afterwards costs no extra round trip and needs no second discovery call.
//
// Thresholds are never written here. They arrive as data from
// `FavnView.Dev.DesignSystem.Audit` on the rendered page, and this file only
// interprets them, so a threshold has exactly one definition in the repository.
(function () {
  "use strict";

  var CONTROL_SELECTOR =
    'a[href], button, [role="button"], input, select, textarea, summary, [tabindex]:not([tabindex="-1"])';
  var MAX_TARGETS_PER_EXAMPLE = 60;
  var LARGE_TEXT_PX = 24;
  var LARGE_BOLD_TEXT_PX = 18.66;
  var BOLD = 600;

  var canvas = document.createElement("canvas").getContext("2d");

  // Favn's palette is authored in oklch, and `color-mix()` computes to oklab, so
  // `getComputedStyle` hands back colours in those spaces. Neither canvas nor any
  // other browser API converts them to sRGB for us, so the conversion happens
  // here. Without it every contrast check would be unmeasurable — which is what
  // it honestly reported before this existed, rather than guessing.
  function normalize(value) {
    if (!value) return null;
    if (value === "transparent") return [0, 0, 0, 0];

    return (
      parseHex(value) ||
      parseRgb(value) ||
      parseOklch(value) ||
      parseOklab(value) ||
      parseSrgb(value) ||
      viaCanvas(value)
    );
  }

  // Named colours and anything else legacy: canvas normalizes those to hex or
  // rgba(). Chrome leaves fillStyle untouched when handed a value it cannot
  // parse, so priming with two different colours makes that detectable instead
  // of silently reading back the previous colour.
  function viaCanvas(value) {
    canvas.fillStyle = "#000000";
    canvas.fillStyle = value;
    var fromBlack = canvas.fillStyle;
    canvas.fillStyle = "#ffffff";
    canvas.fillStyle = value;
    if (fromBlack !== canvas.fillStyle) return null;

    return parseHex(fromBlack) || parseRgb(fromBlack);
  }

  function parseHex(value) {
    var hex = /^#([0-9a-f]{3}|[0-9a-f]{6})$/i.exec(value);
    if (!hex) return null;

    var digits = hex[1];
    if (digits.length === 3) {
      digits = digits[0] + digits[0] + digits[1] + digits[1] + digits[2] + digits[2];
    }
    return [
      parseInt(digits.slice(0, 2), 16),
      parseInt(digits.slice(2, 4), 16),
      parseInt(digits.slice(4, 6), 16),
      1,
    ];
  }

  function parseRgb(value) {
    var rgb = /^rgba?\(([^)]+)\)$/i.exec(value);
    if (!rgb) return null;

    var parts = numbers(rgb[1]);
    if (parts.length < 3) return null;
    return [parts[0], parts[1], parts[2], parts.length > 3 ? parts[3] : 1];
  }

  function parseSrgb(value) {
    var srgb = /^color\(srgb\s+([^)]+)\)$/i.exec(value);
    if (!srgb) return null;

    var parts = numbers(srgb[1]);
    if (parts.length < 3) return null;
    return [
      clamp255(parts[0] * 255),
      clamp255(parts[1] * 255),
      clamp255(parts[2] * 255),
      parts.length > 3 ? parts[3] : 1,
    ];
  }

  function parseOklch(value) {
    var match = /^oklch\(([^)]+)\)$/i.exec(value);
    if (!match) return null;

    var parts = numbers(match[1]);
    if (parts.length < 3) return null;

    var hue = (parts[2] * Math.PI) / 180;
    return oklabToRgb(
      parts[0],
      parts[1] * Math.cos(hue),
      parts[1] * Math.sin(hue),
      parts.length > 3 ? parts[3] : 1
    );
  }

  function parseOklab(value) {
    var match = /^oklab\(([^)]+)\)$/i.exec(value);
    if (!match) return null;

    var parts = numbers(match[1]);
    if (parts.length < 3) return null;

    return oklabToRgb(parts[0], parts[1], parts[2], parts.length > 3 ? parts[3] : 1);
  }

  // Lightness may be written as a percentage, and so may alpha after the slash.
  // Percentages are scaled here so each parser stays flat.
  function numbers(input) {
    var raw = input.trim().split(/[\s,\/]+/).filter(Boolean);
    var values = [];

    for (var index = 0; index < raw.length; index++) {
      var token = raw[index];
      var number = parseFloat(token);
      if (isNaN(number)) return [];
      values.push(token.indexOf("%") !== -1 ? number / 100 : number);
    }

    return values;
  }

  function oklabToRgb(lightness, aStar, bStar, alpha) {
    var l = Math.pow(lightness + 0.3963377774 * aStar + 0.2158037573 * bStar, 3);
    var m = Math.pow(lightness - 0.1055613458 * aStar - 0.0638541728 * bStar, 3);
    var s = Math.pow(lightness - 0.0894841775 * aStar - 1.291485548 * bStar, 3);

    return [
      clamp255(gamma(4.0767416621 * l - 3.3077115913 * m + 0.2309699292 * s) * 255),
      clamp255(gamma(-1.2684380046 * l + 2.6097574011 * m - 0.3413193965 * s) * 255),
      clamp255(gamma(-0.0041960863 * l - 0.7034186147 * m + 1.707614701 * s) * 255),
      alpha,
    ];
  }

  function gamma(channel) {
    if (channel <= 0.0031308) return 12.92 * channel;
    return 1.055 * Math.pow(channel, 1 / 2.4) - 0.055;
  }

  function clamp255(value) {
    return Math.max(0, Math.min(255, Math.round(value)));
  }

  function over(color, backdrop) {
    var alpha = color[3];
    return [
      Math.round(color[0] * alpha + backdrop[0] * (1 - alpha)),
      Math.round(color[1] * alpha + backdrop[1] * (1 - alpha)),
      Math.round(color[2] * alpha + backdrop[2] * (1 - alpha)),
    ];
  }

  function luminance(rgb) {
    var channels = rgb.slice(0, 3).map(function (channel) {
      var ratio = channel / 255;
      return ratio <= 0.03928 ? ratio / 12.92 : Math.pow((ratio + 0.055) / 1.055, 2.4);
    });
    return 0.2126 * channels[0] + 0.7152 * channels[1] + 0.0722 * channels[2];
  }

  function contrast(first, second) {
    if (!first || !second) return null;
    var lighter = Math.max(luminance(first), luminance(second));
    var darker = Math.min(luminance(first), luminance(second));
    return Math.round(((lighter + 0.05) / (darker + 0.05)) * 100) / 100;
  }

  // The used background of an element: the first opaque background-color up the
  // tree, with every translucent layer above it composited back on top.
  // Gradients and images are not sampled, so `approximate` says when the number
  // is an approximation rather than pretending it is exact.
  function backgroundOf(element) {
    var layers = [];
    var approximate = false;
    var source = "canvas";
    var node = element;

    while (node && node.nodeType === 1) {
      var style = getComputedStyle(node);
      if (style.backgroundImage && style.backgroundImage !== "none") approximate = true;

      var color = normalize(style.backgroundColor);
      if (color && color[3] > 0) {
        layers.push(color);
        if (source === "canvas") source = describe(node);
        if (color[3] >= 1) break;
      }
      node = node.parentElement;
    }

    var base = normalize(getComputedStyle(document.documentElement).backgroundColor);
    var opaque = layers.length > 0 && layers[layers.length - 1][3] >= 1;
    var resolved = opaque || (base && base[3] >= 1);
    var rgb = base && base[3] >= 1 ? [base[0], base[1], base[2]] : [255, 255, 255];
    for (var index = layers.length - 1; index >= 0; index--) rgb = over(layers[index], rgb);

    return { rgb: rgb, source: source, approximate: approximate, resolved: !!resolved };
  }

  function describe(element) {
    var name = element.tagName.toLowerCase();
    if (element.id) return name + "#" + element.id;
    var classes = (element.getAttribute("class") || "").trim().split(/\s+/).slice(0, 2);
    return classes[0] ? name + "." + classes.join(".") : name;
  }

  function accessibleName(element) {
    var label = element.getAttribute("aria-label");
    if (label) return label.trim();

    var describedBy = element.getAttribute("aria-labelledby");
    if (describedBy) {
      var target = document.getElementById(describedBy);
      if (target) return (target.textContent || "").trim();
    }

    var title = element.getAttribute("title");
    if (title) return title.trim();

    var text = (element.textContent || "").trim();
    if (text) return text;

    var image = element.querySelector("img[alt], svg title");
    if (image) return (image.getAttribute("alt") || image.textContent || "").trim();

    return "";
  }

  function hasOwnText(element) {
    for (var index = 0; index < element.childNodes.length; index++) {
      var node = element.childNodes[index];
      if (node.nodeType === 3 && node.nodeValue.trim()) return true;
    }
    return false;
  }

  function clippedPixels(element, style) {
    if (style.textOverflow === "ellipsis") return 0;
    if (style.overflowX === "visible" && style.overflowY === "visible") return 0;
    return Math.max(0, element.scrollWidth - element.clientWidth);
  }

  function kindsOf(element, style, isControl, name, zoom) {
    var kinds = [];
    var size = (parseFloat(style.fontSize) || 0) / zoom;
    var weight = parseInt(style.fontWeight, 10) || 400;
    var text = hasOwnText(element);

    if (text && (size >= LARGE_TEXT_PX || (size >= LARGE_BOLD_TEXT_PX && weight >= BOLD))) {
      kinds.push("large_text");
    } else if (text) {
      kinds.push("text");
    }

    if (isControl) kinds.push("control");
    if (isControl && !text && name) kinds.push("icon_control");
    if (identifyingBoundary(element, style, isControl)) kinds.push("boundary");

    return kinds;
  }

  // WCAG non-text contrast applies to a boundary that is *required to identify*
  // the component. A soft badge is identified by its background wash; its
  // hairline border is decoration, and judging it at 3:1 would flag the whole
  // palette. So a border only counts as the identifying boundary on a control
  // whose own background is (near-)transparent — an outline input, an outline
  // button — where the border genuinely is all there is to see. A border whose
  // colour is fully transparent is layout reservation (a ghost button keeping
  // its footprint), not a boundary at all: the control is identified by its
  // text, which the text rules already judge.
  function identifyingBoundary(element, style, isControl) {
    if (!isControl) return false;
    if (!(parseFloat(style.borderTopWidth) > 0)) return false;

    var borderColor = normalize(style.borderTopColor);
    if (borderColor && borderColor[3] === 0) return false;

    var ownBackground = normalize(style.backgroundColor);
    return !ownBackground || ownBackground[3] < 0.5;
  }

  function rectOf(element) {
    var box = element.getBoundingClientRect();
    return [
      Math.round(box.left),
      Math.round(box.top),
      Math.round(box.width),
      Math.round(box.height),
    ];
  }

  // `scale` renders the page zoomed so a screenshot has real pixels rather than
  // an upscaled crop. Zoom multiplies used lengths, so every length metric is
  // divided back out: a control must not pass the target-size rule merely
  // because the page was rendered large. Rects stay in visual pixels, which is
  // what a screenshot crop needs.
  function scale() {
    var node = root();
    var value = node ? parseFloat(node.getAttribute("data-scale")) : 1;
    return value && value > 0 ? value : 1;
  }

  function measure(element, exampleId, index) {
    var style = getComputedStyle(element);
    var box = element.getBoundingClientRect();
    if (box.width === 0 || box.height === 0) return null;
    var zoom = scale();

    var isControl = element.matches(CONTROL_SELECTOR);
    var name = accessibleName(element);
    var kinds = kindsOf(element, style, isControl, name, zoom);
    if (kinds.length === 0) return null;

    // WCAG exempts inactive components from contrast and target size. Which
    // rules honour the exemption is part of the rule data, not decided here.
    var inactive = !!element.closest('[disabled], [aria-disabled="true"]');

    var background = backgroundOf(element);
    var foreground = normalize(style.color);
    var resolvedForeground = foreground ? over(foreground, background.rgb) : null;
    var borderColor = normalize(style.borderTopColor);
    var borderWidth = parseFloat(style.borderTopWidth) || 0;

    var target = {
      ref: exampleId + "#" + index,
      tag: element.tagName.toLowerCase(),
      selector: describe(element),
      text: (element.textContent || "").trim().slice(0, 60),
      kinds: kinds,
      inactive: inactive,
      rect: rectOf(element),
      fg: style.color,
      bg: "rgb(" + background.rgb.join(", ") + ")",
      bg_source: background.source,
      bg_approximate: background.approximate,
      bg_resolved: background.resolved,
      font_size: Math.round((parseFloat(style.fontSize) / zoom) * 100) / 100,
      font_weight: style.fontWeight,
      accessible_name: name,
      accessible_name_length: name.length,
      min_side: Math.round(Math.min(box.width, box.height) / zoom),
      contrast: background.resolved ? contrast(resolvedForeground, background.rgb) : null,
      boundary_contrast:
        background.resolved && borderWidth > 0 && borderColor
          ? contrast(over(borderColor, background.rgb), background.rgb)
          : null,
      unresolved: [],
    };

    if (!foreground) target.unresolved.push("color:" + style.color);
    if (!background.resolved) target.unresolved.push("background: no opaque layer found");
    if (borderWidth > 0 && !borderColor) {
      target.unresolved.push("border-color:" + style.borderTopColor);
    }

    target.checks = evaluate(target, kinds);
    return target;
  }

  function evaluate(measurement, kinds) {
    return rules()
      .filter(function (rule) {
        return kinds.indexOf(rule.applies) !== -1;
      })
      .map(function (rule) {
        if (measurement.inactive && rule.inactive_exempt) {
          return {
            rule: rule.id,
            metric: rule.metric,
            op: rule.op,
            limit: rule.limit,
            value: null,
            status: "skipped",
            reason: "inactive_control",
            why: rule.why,
          };
        }

        var value = measurement[rule.metric];
        if (typeof value !== "number") {
          return {
            rule: rule.id,
            metric: rule.metric,
            op: rule.op,
            limit: rule.limit,
            value: null,
            status: "skipped",
            reason: "not_measured",
            why: rule.why,
          };
        }

        var pass = rule.op === "gte" ? value >= rule.limit : value <= rule.limit;
        return {
          rule: rule.id,
          metric: rule.metric,
          op: rule.op,
          limit: rule.limit,
          value: value,
          status: pass ? "pass" : "fail",
          reason: null,
          why: rule.why,
        };
      });
  }

  function root() {
    return document.getElementById("favn-design-system");
  }

  function rules() {
    var node = root();
    if (!node) return [];
    try {
      return JSON.parse(node.getAttribute("data-audit-rules") || "[]");
    } catch (error) {
      return [];
    }
  }

  function examples(filter) {
    var nodes = Array.prototype.slice.call(document.querySelectorAll("[data-favn-example]"));
    if (!filter) return nodes;
    return nodes.filter(function (node) {
      return node.getAttribute("data-favn-example").indexOf(filter) !== -1;
    });
  }

  function auditExample(node) {
    var id = node.getAttribute("data-favn-example");
    var style = getComputedStyle(node);
    var candidates = Array.prototype.slice.call(node.querySelectorAll("*"));
    var truncated = Math.max(0, candidates.length - MAX_TARGETS_PER_EXAMPLE);
    var targets = [];
    var clipped = clippedPixels(node, style);

    candidates.slice(0, MAX_TARGETS_PER_EXAMPLE).forEach(function (element, index) {
      var elementStyle = getComputedStyle(element);
      clipped = Math.max(clipped, clippedPixels(element, elementStyle));

      var target = measure(element, id, index);
      if (target) targets.push(target);
    });

    var example = {
      id: id,
      component: node.getAttribute("data-favn-component"),
      source: node.getAttribute("data-favn-source"),
      rect: rectOf(node),
      document_rect: documentRect(node),
      visible: visible(node),
      render_error: node.getAttribute("data-favn-error") === "1",
      clipped_px: Math.round(clipped / scale()),
      truncated_targets: truncated,
      targets: targets,
    };

    example.checks = evaluate(example, ["example"]);
    return example;
  }

  function documentRect(element) {
    var box = element.getBoundingClientRect();
    return [
      Math.round(box.left + window.scrollX),
      Math.round(box.top + window.scrollY),
      Math.round(box.width),
      Math.round(box.height),
    ];
  }

  function visible(element) {
    var box = element.getBoundingClientRect();
    return box.bottom > 0 && box.top < window.innerHeight;
  }

  function audit(filter) {
    var collected = examples(filter).map(auditExample);
    var summary = { examples: collected.length, pass: 0, fail: 0, skipped: 0, render_errors: 0 };
    var failures = [];

    collected.forEach(function (example) {
      if (example.render_error) summary.render_errors++;

      collect(example, example.checks, null, summary, failures);
      example.targets.forEach(function (target) {
        collect(example, target.checks, target, summary, failures);
      });
    });

    return {
      version: 1,
      theme: (root() && root().getAttribute("data-theme")) || null,
      scale: scale(),
      url: window.location.href,
      viewport: {
        inner_width: window.innerWidth,
        inner_height: window.innerHeight,
        device_pixel_ratio: window.devicePixelRatio,
        scroll_x: Math.round(window.scrollX),
        scroll_y: Math.round(window.scrollY),
      },
      summary: summary,
      failures: failures,
      examples: collected,
    };
  }

  function collect(example, checks, target, summary, failures) {
    checks.forEach(function (check) {
      summary[check.status]++;
      if (check.status !== "fail") return;

      failures.push({
        example: example.id,
        ref: target ? target.ref : example.id,
        selector: target ? target.selector : null,
        text: target ? target.text : null,
        rule: check.rule,
        value: check.value,
        limit: check.limit,
        fg: target ? target.fg : null,
        bg: target ? target.bg : null,
        bg_approximate: target ? target.bg_approximate : null,
        rect: target ? target.rect : example.rect,
        why: check.why,
      });
    });
  }

  // Verdicts and boxes only: the same walk without the per-target detail, for
  // when the full report is more than the question needs.
  function summary(filter) {
    var report = audit(filter);
    return {
      summary: report.summary,
      viewport: report.viewport,
      failures: report.failures,
      boxes: report.examples.map(function (example) {
        return { id: example.id, rect: example.rect, visible: example.visible };
      }),
    };
  }

  window.favn = window.favn || {};
  window.favn.audit = audit;
  window.favn.summary = summary;
  window.favn.rules = rules;
})();
