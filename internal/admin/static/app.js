(function () {
  var target = document.getElementById("topology");
  if (!target || !window.EventSource) { return; }
  var source = new EventSource("/events");
  source.addEventListener("topology", function () {
    if (window.htmx) { window.htmx.trigger(target, "topology"); }
  });
})();
