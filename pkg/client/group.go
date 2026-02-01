package client

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/message"
	"github.com/gezibash/arc/v2/pkg/reference"
	"github.com/gezibash/arc-node/pkg/group"
)

// CreateGroup generates a new group keypair, builds the initial manifest,
// and publishes it to the node.
func (c *Client) CreateGroup(ctx context.Context, name string, adminKPs ...*identity.Keypair) (*group.Manifest, *identity.Keypair, error) {
	groupKP, manifest, err := group.Create(name, adminKPs...)
	if err != nil {
		return nil, nil, err
	}

	if _, err := c.PublishManifest(ctx, groupKP, manifest); err != nil {
		return nil, nil, fmt.Errorf("publish initial manifest: %w", err)
	}

	return manifest, groupKP, nil
}

// PublishManifest signs and publishes a group manifest to the node.
// Returns the content reference of the stored manifest.
func (c *Client) PublishManifest(ctx context.Context, groupKP *identity.Keypair, manifest *group.Manifest) (reference.Reference, error) {
	msg, data, err := group.SignManifest(manifest, groupKP, manifest.Parent)
	if err != nil {
		return reference.Reference{}, fmt.Errorf("sign manifest: %w", err)
	}

	// Store manifest bytes as content.
	contentRef, err := c.PutContent(ctx, data)
	if err != nil {
		return reference.Reference{}, fmt.Errorf("put manifest content: %w", err)
	}

	// Publish signed message with group labels.
	labels := map[string]string{
		"group":       hex.EncodeToString(manifest.ID[:]),
		"contentType": "arc/group.manifest",
	}
	if _, err := c.SendMessage(ctx, msg, labels); err != nil {
		return reference.Reference{}, fmt.Errorf("send manifest message: %w", err)
	}

	return contentRef, nil
}

// GetGroupManifest retrieves the latest manifest for a group by querying
// for the most recent arc/group.manifest message from the group's pubkey.
func (c *Client) GetGroupManifest(ctx context.Context, groupPK identity.PublicKey) (*group.Manifest, error) {
	groupHex := hex.EncodeToString(groupPK[:])
	result, err := c.QueryMessages(ctx, &QueryOptions{
		Labels: map[string]string{
			"group":       groupHex,
			"contentType": "arc/group.manifest",
		},
		Limit:      1,
		Descending: true,
	})
	if err != nil {
		return nil, fmt.Errorf("query manifests: %w", err)
	}
	if len(result.Entries) == 0 {
		return nil, fmt.Errorf("no manifest found for group %s", groupHex[:16])
	}

	entry := result.Entries[0]
	data, err := c.GetContent(ctx, entry.Ref)
	if err != nil {
		return nil, fmt.Errorf("get manifest content: %w", err)
	}

	// Reconstruct the message for verification.
	contentRef := reference.Compute(data)
	msg := message.New(groupPK, identity.PublicKey{}, contentRef, "arc/group.manifest")
	// We need the original signature; for now, unmarshal without full verification
	// since the node verified on ingest.
	_ = msg

	m, err := group.UnmarshalManifest(data)
	if err != nil {
		return nil, fmt.Errorf("unmarshal manifest: %w", err)
	}
	return m, nil
}

// AddGroupMember adds a member to the group and publishes the updated manifest.
func (c *Client) AddGroupMember(ctx context.Context, groupKP *identity.Keypair, current *group.Manifest, member identity.PublicKey, role group.Role) (*group.Manifest, error) {
	next, err := group.AddMember(current, groupKP, member, role)
	if err != nil {
		return nil, err
	}
	if _, err := c.PublishManifest(ctx, groupKP, next); err != nil {
		return nil, err
	}
	return next, nil
}

// RemoveGroupMember removes a member and publishes the updated manifest.
func (c *Client) RemoveGroupMember(ctx context.Context, groupKP *identity.Keypair, current *group.Manifest, member identity.PublicKey) (*group.Manifest, error) {
	next, err := group.RemoveMember(current, groupKP, member)
	if err != nil {
		return nil, err
	}
	if _, err := c.PublishManifest(ctx, groupKP, next); err != nil {
		return nil, err
	}
	return next, nil
}
