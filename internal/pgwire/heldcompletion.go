package pgwire

// heldCompletion holds back a statement's CommandComplete so the batch's
// commit can fail in its place. Everything else -- the rows, the notices,
// a parameter status -- goes to the client as it is produced; only the
// "this statement finished" is a claim the commit can still falsify.
type heldCompletion struct {
	ResultWriter
	tag  string
	held bool
}

func (h *heldCompletion) CommandComplete(tag string) error {
	h.tag, h.held = tag, true
	return nil
}

func (h *heldCompletion) release() error {
	if !h.held {
		return nil
	}
	h.held = false
	return h.ResultWriter.CommandComplete(h.tag)
}
