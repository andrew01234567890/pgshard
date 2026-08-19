(function () {
  var targets = document.querySelectorAll("#topology, [data-live]");
  if (!targets.length || !window.EventSource) { return; }
  var source = new EventSource("/events");
  source.addEventListener("topology", function () {
    if (!window.htmx) { return; }
    targets.forEach(function (t) { window.htmx.trigger(t, "topology"); });
  });
})();
