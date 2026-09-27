package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// Events contain visible assistant updates and tool activity, never private model reasoning.
type Event struct {
	Type    string   `json:"type"`
	Message *Message `json:"message,omitempty"`
	Call    *Call    `json:"call,omitempty"`
}
type Action struct {
	ID      string `json:"id"`
	Label   string `json:"label"`
	Message string `json:"message"`
}
type Handoff struct {
	Loading    *Loading `json:"loading,omitempty"`
	ID         string   `json:"id"`
	Kind       string   `json:"kind"`
	Prompt     string   `json:"prompt"`
	SecretName string   `json:"secret_name,omitempty"`
	Actions    []Action `json:"actions"`
}
type Input struct {
	Loading   *Loading `json:"loading,omitempty"`
	Message   string   `json:"message"`
	ActionID  string   `json:"action_id,omitempty"`
	HandoffID string   `json:"handoff_id,omitempty"`
}

func askUser(v *Session, arguments string) (string, error) {
	var request struct {
		Kind       string `json:"kind"`
		Prompt     string `json:"prompt"`
		SecretName string `json:"secret_name"`
		Options    []struct {
			Label   string `json:"label"`
			Message string `json:"message"`
		} `json:"options"`
	}
	if err := json.Unmarshal([]byte(arguments), &request); err != nil {
		return "", err
	}
	if strings.TrimSpace(request.Prompt) == "" || len(request.Prompt) > 1500 {
		return "", errors.New("provide a concise prompt, up to 1500 characters")
	}
	h := &Handoff{ID: fmt.Sprint(len(v.Messages)), Kind: request.Kind, Prompt: request.Prompt, SecretName: strings.TrimSpace(request.SecretName)}
	switch request.Kind {
	case "secret":
		if h.SecretName == "" || len(h.SecretName) > 200 {
			return "", errors.New("provide the source secret name")
		}
		h.Actions = []Action{{ID: "retry_secret", Label: "I've added a key, try again", Message: secretReply(h.SecretName)}, {ID: "explain", Label: "Tell me more", Message: "Explain which source credential is needed, why, and how to get it. Do not ask me to paste it into chat. Offer the secret action again when appropriate."}}
	case "choice", "question":
		if len(request.Options) > 4 {
			return "", errors.New("offer at most four options")
		}
		if request.Kind == "choice" && len(request.Options) < 2 {
			return "", errors.New("offer at least two choices")
		}
		for i, option := range request.Options {
			if strings.TrimSpace(option.Label) == "" || len(option.Label) > 80 || strings.TrimSpace(option.Message) == "" || len(option.Message) > 1000 {
				return "", errors.New("each option needs a short label and reply")
			}
			h.Actions = append(h.Actions, Action{ID: fmt.Sprintf("option_%d", i), Label: option.Label, Message: option.Message})
		}
		h.Actions = append(h.Actions, Action{ID: "explain", Label: "Tell me more", Message: "Explain the current question and the tradeoffs, then offer the choices again."})
	default:
		return "", errors.New("kind must be secret, choice, or question")
	}
	v.Pending = h
	return "Waiting for the user. The UI is displaying the prompt and action buttons.", nil
}
func secretReply(name string) string {
	return fmt.Sprintf("I've added or updated the managed source secret %q. Use this exact secret reference and retry the source test.", name)
}

// SaveRequestedSecret never puts the secret value into conversation history or events.
func (s *Service) SaveRequestedSecret(ctx context.Context, id, handoffID, name, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, err := s.load(id)
	if err != nil {
		return err
	}
	if v.PublishedID != "" || v.Pending == nil || v.Pending.Kind != "secret" || v.Pending.ID != handoffID {
		return errors.New("this conversation is not waiting for a secret")
	}
	name = strings.TrimSpace(name)
	if name == "" || len(name) > 200 || value == "" || len(value) > 16000 {
		return errors.New("provide a secret name and value")
	}
	cfg, err := s.settings()
	if err != nil {
		return err
	}
	if name == cfg.APIKeyRef {
		return errors.New("choose a source secret name; the model provider key is separate")
	}
	if err = s.Secrets.Put(ctx, name, []byte(value)); err != nil {
		return err
	}
	v.Pending.SecretName = name
	v.Pending.Actions[0].Message = secretReply(name)
	return s.save(v)
}
