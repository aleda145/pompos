package web

import (
	"context"
	"errors"
	"net/http"
	"time"

	"pompos/internal/compiler"
	runnerpython "pompos/internal/runner/python"
	"pompos/internal/store"
)

func (a *App) ingestionPreview(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	ctx, cancel := context.WithTimeout(r.Context(), runnerpython.EnvironmentPreparationTimeout+10*time.Second)
	defer cancel()
	item, err := a.Store.Get(ctx, r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		a.serverError(w, err)
		return
	}
	item, _ = a.hydrate(item)
	data := struct {
		Preview      *runnerpython.TablePreview
		PreviewError string
	}{PreviewError: runnerpython.PreviewUnavailable}
	if item.LoadError != "" {
		data.PreviewError = item.LoadError
	} else if a.Previewer != nil {
		preview, err := a.Previewer.Preview(ctx, compiler.ExecutionPlan{
			Script: item.Runtime.Script, ScriptDigest: item.Runtime.ScriptDigest,
			Python: item.Runtime.Python, Dependencies: item.Runtime.Dependencies, LockDigest: item.Runtime.LockDigest,
			DestinationType: item.Destination.Type, DestinationPath: item.Destination.Path,
			DestinationSchema: item.Destination.Schema, DestinationObject: item.Destination.Table, SecretRefs: item.Runtime.SecretRefs,
		})
		if err == nil {
			data.Preview = &preview
		}
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := a.templates["detail"].ExecuteTemplate(w, "preview", data); err != nil {
		a.Logger.Printf("render preview: %v", err)
	}
}
