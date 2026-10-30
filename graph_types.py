CATEGORIES = {
 "person": ("person", "individual"), "group": ("person", "group"),
 "organization": ("organization", "individual"), "place": ("place", "place"),
 "physical_instance": ("object", "physical_instance"), "model": ("object", "model"),
 "work": ("object", "work"), "service": ("object", "service"),
 "event": ("activity", "event_instance"), "event_series": ("activity", "event_series"),
 "concept": ("object", "concept"), "unknown": ("unresolved", "unknown"),
}

KINDS = ("fact", "preference", "plan", "negated", "unknown")

SCOPES = ("real", "draft", "fictional", "hypothetical", "uncertain")
