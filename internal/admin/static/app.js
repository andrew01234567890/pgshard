(function () {
  var target = document.querySelector("[data-live]");
  if (!target || !window.EventSource) { return; }
  var source = new EventSource("/events");
  source.addEventListener("topology", function () {
    if (window.htmx) { window.htmx.trigger(target, "topology"); }
  });
})();
