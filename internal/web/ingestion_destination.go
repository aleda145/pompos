package web

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"

	"pompos/internal/agent"
	"pompos/internal/destination"
	"pompos/internal/spec"
	"pompos/internal/store"
)

func (a *App) updateIngestionDestination(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Invalid destination request", http.StatusBadRequest)
		return
	}
	id, err := a.setIngestionDestination(r.Context(), r.PathValue("id"), r.FormValue("destination"), r.FormValue("format"))
	if err != nil {
		item, loadErr := a.getIngestion(r.Context(), r.PathValue("id"))
		if loadErr != nil {
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
			return
		}
		page := a.detailData(r, item)
		page.DestinationError = err.Error()
		page.DestinationValue, page.DestinationFormat = r.FormValue("destination"), r.FormValue("format")
		a.render(w, http.StatusUnprocessableEntity, "detail", page)
		return
	}
	http.Redirect(w, r, "/ingestions/"+id+"?destination=saved", http.StatusSeeOther)
}

func (a *App) setIngestionDestination(ctx context.Context, id, name, format string) (string, error) {
	a.publicationMu.Lock()
	defer a.publicationMu.Unlock()
	guard, ok := a.Scheduler.(interface{ WithPublication(func() error) error })
	if !ok {
		return "", errors.New("scheduler does not support changing destinations")
	}
	var targetID string
	err := guard.WithPublication(func() error {
		active, err := a.Store.HasActiveRuns(ctx, id)
		if err != nil {
			return err
		}
		if active {
			return errors.New("this ingestion has queued or running work; wait for it to finish before changing destination")
		}
		item, err := a.Store.Get(ctx, id)
		if err != nil {
			return err
		}
		doc, originalYAML, err := spec.Read(item.SpecPath)
		if err != nil {
			return err
		}
		oldDoc := doc
		dest, err := a.Destinations.GetDestination(ctx, name)
		if err != nil {
			return err
		}
		if err := dest.Validate(); err != nil {
			return err
		}
		if (dest.Type == "objects") != (doc.Destination.Type == "objects") {
			return errors.New("switching between rows and objects requires editing the extractor")
		}
		if !destination.IsFile(dest.Type) {
			format = ""
		}
		if err := destination.ValidateFormat(dest.Type, format); err != nil {
			return err
		}
		doc.Destination.Type, doc.Destination.Format = dest.Type, format
		doc.Destination.Path = dest.OutputPath(doc.Destination.Schema, doc.Destination.Object, format)
		if destination.IsFile(dest.Type) {
			doc.Materialization = spec.Materialization{Strategy: "replace"}
		}
		for _, ref := range dest.SecretRefs {
			if !slices.Contains(doc.Runtime.SecretRefs, ref) {
				doc.Runtime.SecretRefs = append(doc.Runtime.SecretRefs, ref)
			}
		}
		targetID = dest.Name + "/" + destination.SchemaName(doc.Destination.Schema) + "/" + doc.Destination.Object
		targetSpec := item.SpecPath
		if targetID != id {
			if _, err := a.Store.Get(ctx, targetID); err == nil {
				return fmt.Errorf("ingestion already exists at %s", targetID)
			} else if !errors.Is(err, store.ErrNotFound) {
				return err
			}
			targetSpec = spec.ArtifactPath(a.SpecDir, targetID, ".yaml")
			doc.Runtime.Script, err = filepath.Abs(spec.ArtifactPath(a.SpecDir, targetID, ".py"))
			if err != nil {
				return err
			}
			for _, path := range []string{targetSpec, doc.Runtime.Script, doc.Runtime.Script + ".lock"} {
				if _, err := os.Lstat(path); err == nil {
					return fmt.Errorf("destination file already exists: %s", path)
				} else if !errors.Is(err, os.ErrNotExist) {
					return err
				}
			}
		}
		data, err := spec.Marshal(doc)
		if err != nil {
			return err
		}
		updated := spec.ToProjection(doc, targetID, targetSpec, spec.Digest(data))
		if err := a.Scheduler.Validate(updated.Schedule); err != nil {
			return err
		}
		type artifact struct {
			oldPath, newPath string
			data             []byte
		}
		files := []artifact{{item.SpecPath, targetSpec, originalYAML}}
		if targetID != id {
			for _, suffix := range []string{"", ".lock"} {
				content, err := os.ReadFile(oldDoc.Runtime.Script + suffix)
				if suffix == ".lock" && errors.Is(err, os.ErrNotExist) {
					continue
				}
				if err != nil {
					return err
				}
				files = append(files, artifact{oldDoc.Runtime.Script + suffix, doc.Runtime.Script + suffix, content})
			}
		}
		currentID := id
		persist := func(ctx context.Context, nextID, path, digest string) error {
			if nextID == currentID {
				return a.Store.UpdateSpecReference(ctx, nextID, path, digest)
			}
			mover, ok := a.Store.(interface {
				MoveIngestion(context.Context, string, string, string, string) error
			})
			if !ok {
				return errors.New("store does not support changing destinations")
			}
			if err := mover.MoveIngestion(ctx, currentID, nextID, path, digest); err != nil {
				return err
			}
			currentID = nextID
			return nil
		}
		rollback := func(cause error) error {
			var restoreErr error
			for _, file := range files {
				restoreErr = errors.Join(restoreErr, agent.WriteFile(file.oldPath, file.data))
			}
			restoreErr = errors.Join(restoreErr, persist(context.WithoutCancel(ctx), id, item.SpecPath, spec.Digest(originalYAML)))
			restoreErr = errors.Join(restoreErr, a.Scheduler.Upsert(spec.ToProjection(oldDoc, id, item.SpecPath, spec.Digest(originalYAML))))
			if targetID != id && restoreErr == nil {
				for _, file := range files {
					if err := os.Remove(file.newPath); err != nil && !errors.Is(err, os.ErrNotExist) {
						restoreErr = errors.Join(restoreErr, err)
					}
				}
			}
			return errors.Join(cause, restoreErr)
		}
		// Copy only the extractor and its dependency lock. Destination-specific
		// dlt state and pending loads stay with the old destination.
		for _, file := range files[1:] {
			if err := agent.WriteFile(file.newPath, file.data); err != nil {
				return rollback(err)
			}
		}
		if err := agent.WriteFile(targetSpec, data); err != nil {
			return rollback(err)
		}
		if err := persist(ctx, targetID, targetSpec, updated.SpecDigest); err != nil {
			return rollback(err)
		}
		if err := a.Scheduler.Upsert(updated); err != nil {
			return rollback(err)
		}
		if targetID != id {
			for _, file := range files {
				if err := os.Remove(file.oldPath); err != nil {
					return rollback(err)
				}
			}
		}
		a.Logger.Printf("destination updated ingestion_id=%s target_id=%s format=%s", id, targetID, format)
		return nil
	})
	return targetID, err
}
