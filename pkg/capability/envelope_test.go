package capability

import (
	"context"
	"errors"
	"testing"
)

func TestOK(t *testing.T) {
	t.Run("with data", func(t *testing.T) {
		payload := []byte("hello")
		resp := OK(payload)
		if resp == nil {
			t.Fatal("expected non-nil response")
		}
		if string(resp.Payload) != "hello" {
			t.Errorf("Payload = %q, want %q", resp.Payload, "hello")
		}
		if resp.Error != "" {
			t.Errorf("Error = %q, want empty", resp.Error)
		}
		if resp.NoReply {
			t.Error("NoReply = true, want false")
		}
		if resp.Labels != nil {
			t.Errorf("Labels = %v, want nil", resp.Labels)
		}
	})

	t.Run("with nil data", func(t *testing.T) {
		resp := OK(nil)
		if resp == nil {
			t.Fatal("expected non-nil response")
		}
		if resp.Payload != nil {
			t.Errorf("Payload = %v, want nil", resp.Payload)
		}
		if resp.Error != "" {
			t.Errorf("Error = %q, want empty", resp.Error)
		}
		if resp.NoReply {
			t.Error("NoReply = true, want false")
		}
	})
}

func TestOKWithLabels(t *testing.T) {
	t.Run("with payload and labels", func(t *testing.T) {
		payload := []byte("data")
		labels := map[string]string{"content-type": "text/plain", "status": "ok"}
		resp := OKWithLabels(payload, labels)
		if resp == nil {
			t.Fatal("expected non-nil response")
		}
		if string(resp.Payload) != "data" {
			t.Errorf("Payload = %q, want %q", resp.Payload, "data")
		}
		if len(resp.Labels) != 2 {
			t.Fatalf("Labels length = %d, want 2", len(resp.Labels))
		}
		if resp.Labels["content-type"] != "text/plain" {
			t.Errorf("Labels[content-type] = %q, want %q", resp.Labels["content-type"], "text/plain")
		}
		if resp.Labels["status"] != "ok" {
			t.Errorf("Labels[status] = %q, want %q", resp.Labels["status"], "ok")
		}
		if resp.Error != "" {
			t.Errorf("Error = %q, want empty", resp.Error)
		}
		if resp.NoReply {
			t.Error("NoReply = true, want false")
		}
	})

	t.Run("with nil labels", func(t *testing.T) {
		resp := OKWithLabels([]byte("data"), nil)
		if resp == nil {
			t.Fatal("expected non-nil response")
		}
		if string(resp.Payload) != "data" {
			t.Errorf("Payload = %q, want %q", resp.Payload, "data")
		}
		if resp.Labels != nil {
			t.Errorf("Labels = %v, want nil", resp.Labels)
		}
	})
}

func TestErr(t *testing.T) {
	t.Run("with message", func(t *testing.T) {
		resp := Err("something went wrong")
		if resp == nil {
			t.Fatal("expected non-nil response")
		}
		if resp.Error != "something went wrong" {
			t.Errorf("Error = %q, want %q", resp.Error, "something went wrong")
		}
		if resp.Payload != nil {
			t.Errorf("Payload = %v, want nil", resp.Payload)
		}
		if resp.NoReply {
			t.Error("NoReply = true, want false")
		}
	})

	t.Run("with empty message", func(t *testing.T) {
		resp := Err("")
		if resp == nil {
			t.Fatal("expected non-nil response")
		}
		if resp.Error != "" {
			t.Errorf("Error = %q, want empty", resp.Error)
		}
	})
}

func TestNoReply(t *testing.T) {
	resp := NoReply()
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
	if !resp.NoReply {
		t.Error("NoReply = false, want true")
	}
	if resp.Payload != nil {
		t.Errorf("Payload = %v, want nil", resp.Payload)
	}
	if resp.Error != "" {
		t.Errorf("Error = %q, want empty", resp.Error)
	}
	if resp.Labels != nil {
		t.Errorf("Labels = %v, want nil", resp.Labels)
	}
}

func TestHandlerFunc(t *testing.T) {
	t.Run("implements Handler interface", func(t *testing.T) {
		var h Handler = HandlerFunc(func(ctx context.Context, env *Envelope) (*Response, error) {
			return OK(env.Payload), nil
		})

		env := &Envelope{Payload: []byte("test")}
		resp, err := h.Handle(context.Background(), env)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		if string(resp.Payload) != "test" {
			t.Errorf("Payload = %q, want %q", resp.Payload, "test")
		}
	})

	t.Run("returning error", func(t *testing.T) {
		expectedErr := errors.New("internal failure")
		h := HandlerFunc(func(ctx context.Context, env *Envelope) (*Response, error) {
			return nil, expectedErr
		})

		resp, err := h.Handle(context.Background(), &Envelope{})
		if !errors.Is(err, expectedErr) {
			t.Errorf("error = %v, want %v", err, expectedErr)
		}
		if resp != nil {
			t.Errorf("response = %v, want nil", resp)
		}
	})

	t.Run("returning nil response", func(t *testing.T) {
		h := HandlerFunc(func(ctx context.Context, env *Envelope) (*Response, error) {
			return nil, nil
		})

		resp, err := h.Handle(context.Background(), &Envelope{})
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		if resp != nil {
			t.Errorf("response = %v, want nil", resp)
		}
	})
}
